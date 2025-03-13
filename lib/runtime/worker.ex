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
  alias Skitter.Runtime.NodeStore
  require Skitter.Runtime.NodeStore
  alias Skitter.Runtime.FailureBackupNode
  require Skitter.Runtime.FailureBackupNode

  @type t :: %__MODULE__.EpochMetadata{
    current_epoch: number(),
    epochs_recieved: Operation.port_name(),
    msg_queue: queue(any())
  }
  @enforce_keys []
  defstruct current_epoch: 0, epochs_recieved: %{}, msg_queue: :queue.new

  defstruct [:operation, :strategy, :context, :idx, :ref, :state, :role, :epoch_metadata]

  def start_link(args), do: GenServer.start_link(__MODULE__, args)
  def deploy_complete(pid), do: GenServer.cast(pid, :sk_deploy_complete)
  def send_epoch(pid, content), do: GenServer.cast(pid, {:sk_epoch, content})
  def send_backup(pid, content), do: GenServer.cast(pid, {:sk_backup, content})

  @impl true
  def init({context = %{_skr: {:deploy, ref, idx}}, state, role}) do
    context = %{context | _skr: {ref, idx}}
    {:ok, {:uninitialized, [], srv_state(context, state, role, ref, idx)}}
  end

  def init({context = %{_skr: {:redeploy, ref, idx}}, backup_ref, role}) do
    context = %{context | _skr: {ref, idx}}
    FailureBackupNode.fetch_backup(backup_ref, role, context)
    {:ok, {:backup_wait, [], srv_state(context, state, role, ref, idx)}}
  end

  def init({context, state, role}) do
    {ref, idx} = context._skr
    {:ok, srv_state(context, state, role, ref, idx)}
  end

  @impl true
  def handle_cast(:sk_deploy_complete, {:uninitialized, msgs, srv}) do
    srv = put_in(srv.context.deployment, NodeStore.get(:deployment, srv.ref, srv.idx))
    queue = if Skitter.Operation.in_ports(context.operation) == [], do: [{:sk_msg, :start}|msgs], else: msgs 
    {:noreply, queue |> Enum.reverse() |> Enum.reduce(srv, &elem(handle_cast(&1),1))}
  end
  def handle_cast(:sk_deploy_complete, {:backup_wait, msgs, srv}) do
    {:noreply, {:uninitialized, msgs, %{srv | state: state}}}
  end
  def handle_cast(:sk_deploy_complete, srv) do
    Logger.error("Initialized worker received :_sk_deploy_complete message")
    {:noreply, srv}
  end

  def handle_cast({:sk_msg, msg}, {:uninitialized, msgs, srv}) do
    {:noreply, {:uninitialized, [{:sk_msg, msg} | msgs], srv}}
  end
  def handle_cast({:sk_epoch, msg}, {:backup_wait, msgs, srv}) do
    {:noreply, {:backup_wait, [{:sk_epoch, msg} | msgs], srv}}
  end
  def handle_cast({:sk_msg, msg}, srv), do: {:noreply, process_hook(msg, srv)}

  def handle_cast({:sk_epoch, msg}, {:uninitialized, msgs, srv}) do
    {:noreply, {:uninitialized, [{:sk_epoch, msg} | msgs], srv}}
  end
  def handle_cast({:sk_epoch, msg}, {:backup_wait, msgs, srv}) do
    {:noreply, {:backup_wait, [{:sk_epoch, msg} | msgs], srv}}
  end
  def handle_cast({:sk_epoch, msg}, srv), do: {:noreply, process_epoch(msg, srv)} # TODO: wrap in SRV

  def handle_cast({:sk_backup, state}, {:backup_wait, msgs, srv}) do
    {:noreply, {:uninitialized, msgs, %{srv | state: state}}}
  end
  def handle_cast({:sk_backup, state}, {:uninitialized, msgs, srv}) do
    srv = put_in(srv.context.deployment, NodeStore.get(:deployment, srv.ref, srv.idx))
    queue = if Skitter.Operation.in_ports(context.operation) == [], do: [{:sk_msg, :start}|msgs], else: msgs 
    {:noreply, queue |> Enum.reverse() |> Enum.reduce(%{srv | state: state}, &elem(handle_cast(&1),1))}
  end

  def handle_cast(:sk_stop, state), do: {:stop, :normal, state}

  @impl true
  def handle_info(msg, srv), do: {:noreply, process_hook(msg, srv)}

  defp srv_state(context, state, role, ref, idx) when is_function(state, 0) do
    srv_state(context, state.(), role, ref, idx)
  end

  defp srv_state(context, state, role, ref, idx) do
    Telemetry.emit(
      [:worker, :init],
      %{},
      %{pid: self(), context: context, state: state, role: role}
    )

    if Skitter.Operation.in_ports(context.operation) == [], do:
      Skitter.Worker.send(self(), :start) #kickstart source

    %__MODULE__{
      operation: context.operation,
      strategy: context.strategy,
      context: context,
      state: state,
      epoch_metadata: %__MODULE__.EpochMetadata{}
      ref: ref,
      idx: idx,
      role: role
    }
  end

  defp process_hook(msg, srv) do
    state =
      Telemetry.wrap [:hook, :process], %{
        pid: self(),
        context: srv.context,
        message: msg,
        state: srv.state,
        role: srv.role
      } do
        srv.strategy.process(%{srv.context | _epoch: srv.epoch_metadata.current_epoch}, msg, srv.state, srv.role)
      end

    %{srv | state: state}
  end

  def pass_epoch(
      context
      epoch_tick, 
      current_role
  ) do
    graph_context = context.strategy_dag()
    {_, roles_out, level_c, _} = Map.get(graph_context, current_role)
    roles_out |> Enum.each(fn 
      {:out} ->  [{:epoch, epoch_tick, Map.get(graph_context, {:out})}] 
        |> to_all_ports 
        |> emit
      role -> Map.get(graph_context, role)
        |> elem(3) 
        |> Enum.each(&send(&1, %Skitter.Token{port: {:inner, current_role}, value: {:epoch, epoch_tick, level_c}}))
    end)
  end

  #new epoch, non recieved yet => initialize blocking until all epochs recieved
  def process_epoch(
      {port, epoch_tick, upstream_c} = data
      srv
  ) when srv.epoch_metadata.epochs_recieved == %{} and srv.epoch_metadata.current_epoch + 1 == epoch_tick do
    initialized_map = srv.context.strategy_dag() |> Map.get(srv.role) |> elem(0) |> Map.new(&{&1, 0})
    process_epoch( %{srv | epoch_metadata : %{srv.epoch_metadata | epochs_recieved: initialized_map}}, data)
  end
  
  #new epoch, some recieved => keep blocking untill all epochs recieved
  def process_epoch(
      {port, epoch_tick, upstream_c} = data, 
      srv
  ) when srv.epoch_metadata.current_epoch + 1 == epoch_tick do
    {_, new_epoch_map} = srv.epoch_metadata.epochs_recieved |> Map.get_and_update!(port, 
      fn 
        nil -> if upstream_c == 1, do: :pop, else: {upstream_c, upstream_c - 1}
        0 when upstream_c==1 -> :pop #new upstream but immediately removed because only expecting 1 (this one epoch)
        1 -> :pop #removed because last expecting epoch on this port
        0 -> {upstream_c, upstream_c - 1} #new upstream - 1 (just received this epoch) 
        current_upstream_c -> {current_upstream_c, current_upstream_c - 1} # -1 for new recieved epoch on this port
      end)
    check_epoch_map(%{srv | epoch_metadata : %{srv.epoch_metadata | epochs_recieved: new_epoch_map}})
  end

  # all epochs recieved => take snapshot, pass epoch to next role/strategy and fold msg queue
  defp check_epoch_map(%Skitter.Runtime.Worker{
    context: context,
    state: state,
    epoch_metadata: epoch_metadata
    role: role
  } = srv) when epoch_metadata.epochs_recieved == %{} do
    epoch_tick = epoch_metadata.current_epoch + 1
    FailureBackupNode.node_snapshot(self(), role, context, epoch_tick, state)
    pass_epoch(context, epoch_tick, role)
    new_srv = %{srv | epoch_metadata: %__MODULE__.EpochMetadata{current_epoch: epoch_tick}}
    :queue.fold(&handle_cast(&1, &2), new_srv, epoch_metadata.msg_queue)
  end

  # not all epochs recieved
  defp check_epoch_map(srv) do: srv
  
end
