# Copyright 2018 - 2022, Mathijs Saey, Vrije Universiteit Brussel

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

defmodule Skitter.Runtime.Worker do
  @moduledoc """
  This module defines a GenServer that specifies the behaviour of Skitter Workers.

  ## Worker Initialisation

  Workers that are created inside the deploy hook of a strategy may not perform any processing
  until they receive the `:sk_deploy_complete` message. This is done to avoid any processing being
  done before the entire workflow has finished deployment. It also ensures that the emit hook
  of downstream nodes in the workflow can not be called before their deployment is finished.

  The Skitter runtime guarantees that the `:sk_deploy_complete` message is sent in reverse
  topological order so that all the nodes downstream of a node are ready to receive data once a
  node activates.

  Before the deployment of a workflow is complete, the Skitter runtime sets the `_skr` field of
  the `t:Skitter.Strategy.Context/0` to `{:deploy, _, _}`. Thus, when a worker is spawned, it
  checks the value of this field to see if it can finish initialisation.

  Once deployment is completed, the `:_sk_deploy_complete` message is sent to all workers spawned
  by a strategy. This message indicates the worker may begin processing received messages. It is
  possible that workers receive messages before this point. This occurs when messages are sent
  inside the deploy hook. Intra-strategy messages may also be received by a worker before it
  receives the `:_sk_deploy_complete` hook. To ensure this does not cause issues, workers buffer
  all messages received before the initial `:_sk_deploy_complete`. When `:_sk_deploy_complete` is
  received, all these messages are processed in the order of arrival.
  """
  use GenServer, restart: :transient
  require Logger

  use Skitter.Telemetry
  alias Skitter.Runtime.{
    NodeStore,
    BackupStore,
    Emit
  }
  require Skitter.Runtime.NodeStore
  require Skitter.Runtime.ConstantStore

  @type t :: %__MODULE__.EpochMetadata{
    epochs_recieved: Operation.port_name(),
    msg_queue: any()
  }

  defmodule EpochMetadata do
    @enforce_keys []
    defstruct epochs_recieved: %{}, msg_queue: :queue.new, make_checkpoint: nil, recover_checkpoint: nil

    def new(srv) do 
      %__MODULE__{
        make_checkpoint: srv.epoch_metadata.make_checkpoint,
        recover_checkpoint: srv.epoch_metadata.recover_checkpoint
      }
    end
  end

  defstruct [:operation, :strategy, :context, :idx, :ref, :state, :role, :epoch_metadata]

  def start_link(args), do: GenServer.start_link(__MODULE__, args)
  def deploy_complete(pid), do: GenServer.cast(pid, :sk_deploy_complete)
  def send_backup(pid, content), do: GenServer.cast(pid, {:sk_backup, content})
  def deliver_epoch(graph_context, content) do 
    Enum.each(graph_context.in , fn 
      role -> Map.get(graph_context.nodes, role).pids
        |> Enum.each(&GenServer.cast(&1, {:sk_epoch, content}))
    end)
  end

  @impl true
  def init({context = %{_skr: {:deploy, ref, idx}}, state, role}) do
    context = %{context | _skr: {ref, idx}}
    {:ok, {:uninitialized, [], srv_state(context, state, role, ref, idx)}}
  end

  def init({context = %{_skr: {:redeploy, ref, idx}}, %BackupStore.Reference{} = backup_ref, role}) do
    context = %{context | _skr: {ref, idx}}
    BackupStore.fetch_backup(context, backup_ref, role)
    {:ok, {:backup_wait, [], srv_state(context, nil, role, ref, idx)}}
  end

  def init({context = %{_skr: {:redeploy, ref, idx}}, state, role}) do
    context = %{context | _skr: {ref, idx}}
    {:ok, {:uninitialized, [], srv_state(context, state, role, ref, idx)}}
  end

  def init({context, state, role}) do
    {ref, idx} = context._skr
    {:ok, srv_state(context, state, role, ref, idx)}
  end

  @impl true
  def handle_cast(:sk_deploy_complete, {:uninitialized, msgs, srv}) do
    srv = put_in(srv.context.deployment, NodeStore.get(:deployment, srv.ref, srv.idx))
    srv = put_in(srv.context.strategy_dag, NodeStore.get(:dag, srv.ref, srv.idx))
    srv = update_in(srv.context._epoch, fn 
      nil -> 0
      e -> e
    end)

    queue = if Skitter.Operation.in_ports(srv.context.operation) == [], do: [:sk_start|msgs], else: msgs 
    {:noreply, queue |> Enum.reverse() |> Enum.reduce(srv, &elem(handle_cast(&1, &2),1))}
  end
  def handle_cast(:sk_deploy_complete, {:backup_wait, msgs, srv}) do
    {:noreply, {:uninitialized, msgs, srv}}
  end
  def handle_cast(:sk_deploy_complete, srv) do
    Logger.error("Initialized worker received :_sk_deploy_complete message")
    {:noreply, srv}
  end

  def handle_cast({:sk_msg, msg, epoch}, {uninit_tag, msgs, srv}) do
    {:noreply, {uninit_tag, [{:sk_msg, msg, epoch} | msgs], srv}}
  end
  def handle_cast({:sk_msg, msg, epoch}, srv), do: {:noreply, process_hook(msg, srv, epoch)}

  def handle_cast({:sk_epoch, msg}, {uninit_tag, msgs, srv}) do
    {:noreply, {uninit_tag, [{:sk_epoch, msg} | msgs], srv}}
  end
  def handle_cast({:sk_epoch, msg}, srv) do
    {:noreply, process_epoch(msg, srv)} 
  end

  def handle_cast({:sk_backup, {state_epoch, state}}, {:backup_wait, msgs, srv}) do
    context = %{srv.context | _epoch: state_epoch}
    state = srv.epoch_metadata.recover_checkpoint.(context, state, state_epoch, srv.role)
    {:noreply, {:uninitialized, msgs, %{srv | state: state, context: context}}}
  end
  def handle_cast({:sk_backup, {state_epoch, state}}, {:uninitialized, msgs, srv}) do
    srv = put_in(srv.context.deployment, NodeStore.get(:deployment, srv.ref, srv.idx))
    srv = put_in(srv.context._epoch, state_epoch)
    srv = put_in(srv.state, srv.epoch_metadata.recover_checkpoint.(srv.context, state, state_epoch, srv.role))
    srv = put_in(srv.epoch_metadata, __MODULE__.EpochMetadata.new(srv))
    srv = put_in(srv.context.strategy_dag, NodeStore.get(:dag, srv.ref, srv.idx))
    queue = if Skitter.Operation.in_ports(srv.context.operation) == [], do: [:sk_emit_epoch,{:sk_msg, :play, state_epoch}|msgs], else: msgs 
    {:noreply, queue |> Enum.reverse() |> Enum.reduce(srv, &elem(handle_cast(&1, &2),1))}
  end

  def handle_cast(:sk_start, srv) do 
    {:noreply, [:sk_emit_epoch, {:sk_msg, :play, srv.context._epoch + 1}] |> Enum.reduce(srv, &elem(handle_cast(&1, &2), 1))}
  end
  
  def handle_cast(:sk_stop, state), do: {:stop, :normal, state}

  def handle_cast(msg, state), do: handle_info(msg, state)

  def handle_info(:sk_emit_epoch, srv) do 
    srv = put_in(srv.epoch_metadata, __MODULE__.EpochMetadata.new(srv))
    srv = update_in(srv.context._epoch, &(&1 + 1))
    BackupStore.node_snapshot(self(), srv.role, srv.context, srv.epoch_metadata.make_checkpoint.(srv.context, srv.state, srv.context._epoch, srv.role))
    pass_epoch(srv)
    Process.send_after(self(), :sk_emit_epoch, 2000)
    {:noreply, srv}
  end

  defp srv_state(context, state, role, ref, idx) when is_function(state, 0) do
    srv_state(context, state.(), role, ref, idx)
  end

  defp srv_state(context, state, role, ref, idx) do
    Telemetry.emit(
      [:worker, :init],
      %{},
      %{pid: self(), context: context, state: state, role: role}
    )

    mod_info = context.strategy.__info__(:functions)
    make_checkpoint = if Keyword.get(mod_info, :make_checkpoint) == 4, do: &context.strategy.make_checkpoint/4, else: fn (_ctx, state, _epoch, _role) -> state end
    recover_checkpoint = if Keyword.get(mod_info, :recover_checkpoint) == 4, do: &context.strategy.recover_checkpoint/4, else: fn (_ctx, state, _epoch, _role) -> state end

    %__MODULE__{
      operation: context.operation,
      strategy: context.strategy,
      context: context,
      state: state,
      epoch_metadata: %__MODULE__.EpochMetadata{
        make_checkpoint: make_checkpoint,
        recover_checkpoint: recover_checkpoint
      },
      ref: ref,
      idx: idx,
      role: role
    }
  end

  defp process_hook(msg, srv, epoch) when srv.context._epoch + 1 != epoch do 
    if Skitter.Operation.in_ports(srv.context.operation) == [] do
      process_hook(msg, srv, epoch+1)
    else 
      update_in(srv.epoch_metadata.msg_queue, &:queue.in({:sk_msg, msg, epoch}, &1))
    end
  end

  defp process_hook(msg, srv, _epoch) do
    state =
      Telemetry.wrap [:hook, :process], %{
        pid: self(),
        context: srv.context,
        message: msg,
        state: srv.state,
        role: srv.role
      } do
        srv.strategy.process(srv.context, msg, srv.state, srv.role)
      end

    %{srv | state: state}
  end

  def pass_epoch(
    %Skitter.Runtime.Worker{
      context: context,
      role: current_role
    } 
  ) do
    graph_context = context.strategy_dag
    role_node = Map.get(graph_context.nodes, current_role)
    role_node.out |> Enum.each(fn 
      {:out} ->  Emit.emit_epoch(context, {context._epoch, graph_context.out_count})
      role -> Map.get(graph_context.nodes, role).pids
        |> Enum.each(&GenServer.cast(&1, {:sk_epoch, {{:inner, current_role}, context._epoch, role_node.pids_len}}))
    end)
  end

  #new epoch, non recieved yet => initialize blocking until all epochs recieved
  def process_epoch(
      {_, epoch_tick, _} = data,
      srv
  ) when srv.epoch_metadata.epochs_recieved == %{} and srv.context._epoch + 1 == epoch_tick do
    initialized_map = Map.get(srv.context.strategy_dag.nodes, srv.role).in |> Map.new(&{&1, 0})
    process_epoch(data, %{srv | epoch_metadata: %{srv.epoch_metadata | epochs_recieved: initialized_map}})
  end
  
  #new epoch, some recieved => keep blocking untill all epochs recieved
  def process_epoch(
      {port, epoch_tick, upstream_c}, 
      srv
  ) when srv.context._epoch + 1 == epoch_tick do
    {_, new_epoch_map} = srv.epoch_metadata.epochs_recieved |> Map.get_and_update!(port, 
      fn 
        nil -> if upstream_c == 1, do: :pop, else: {upstream_c, upstream_c - 1}
        0 when upstream_c==1 -> :pop #new upstream but immediately removed because only expecting 1 (this one epoch)
        1 -> :pop #removed because last expecting epoch on this port
        0 -> {upstream_c, upstream_c - 1} #new upstream - 1 (just received this epoch) 
        current_upstream_c -> {current_upstream_c, current_upstream_c - 1} # -1 for new recieved epoch on this port
      end)
    check_epoch_map(%{srv | epoch_metadata: %{srv.epoch_metadata | epochs_recieved: new_epoch_map}})
  end

  def process_epoch(msg, srv), do: update_in(srv.epoch_metadata.msg_queue, &:queue.in({:sk_epoch, msg}, &1))

  # all epochs recieved => take snapshot, pass epoch to next role/strategy and fold msg queue
  defp check_epoch_map(%Skitter.Runtime.Worker{
    state: state,
    epoch_metadata: epoch_metadata,
    role: role
  } = srv) when epoch_metadata.epochs_recieved == %{} do
    srv = update_in(srv.context._epoch, &(&1 + 1))
    srv = put_in(srv.epoch_metadata, __MODULE__.EpochMetadata.new(srv))
    BackupStore.node_snapshot(self(), role, srv.context, srv.epoch_metadata.make_checkpoint.(srv.context, state, srv.context._epoch, srv.role))
    pass_epoch(srv)
    :queue.fold(&elem(handle_cast(&1, &2),1), srv, epoch_metadata.msg_queue)
  end
 
  # not all epochs recieved
  defp check_epoch_map(srv), do: srv
  
end
