use Skitter

defmodule Skitter.Runtime.BackupStore do
  use GenServer
  require Logger
  require MapMacro
  alias Skitter.Strategy.Context
  alias Skitter.Runtime, as: RT 
  alias Skitter.Runtime.{
    NodeStore,
    ConstantStore,
    FailureObs
  }
  alias Skitter.Config
  require NodeStore
  require ConstantStore

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg)
  end

  def init({ref, nodes}) do 
    FailureObs.put_store_pid(ref, self(), Node.self())
    Logger.info("Backup Store started", [deployment: ref])
    { :ok, 
      { 0, # lowest epoch
        Map.new(), # backup states 
        nodes, # set of ops without deployment info
        %{1=>0}, # epoch => current snapshot revieved count
        0 # total expected counts for epoch 1
    }}
  end

  # use handle_continue() instead
  def admin_cast(pid, msg), do: GenServer.cast(pid, msg)
  # def admin_cast(ctx, ids, msg) when is_list(ids), do: Enum.map(ids, &admin_cast(ctx, &1, msg))
  def admin_cast(%Context{_skr: {ref,_}}, id, msg), do: GenServer.cast(NodeStore.get(:failure_stores, ref, id), msg)
  def admin_cast(%Context{_skr: {_, ref,_}}, id, msg), do: GenServer.cast(NodeStore.get(:failure_stores, ref, id), msg)
  def admin_cast(ref, id, msg), do: GenServer.cast(NodeStore.get(:failure_stores, ref, id), msg)

  def admin_broadcast(%Context{_skr: {ref,_}}, msg), do: NodeStore.get_all(:failure_stores, ref) |> Enum.map(& GenServer.cast(&1, msg))
  def admin_broadcast(%Context{_skr: {_, ref,_}}, msg), do: NodeStore.get_all(:failure_stores, ref) |> Enum.map(&GenServer.cast(&1, msg))
  def admin_broadcast(ref, msg), do: NodeStore.get_all(:failure_stores, ref) |> Enum.map(&GenServer.cast(&1, msg))
  
  def start_backup(pid, role, context = %Context{_skr: {ref,_}}, state) do
    node_idx = ConstantStore.get(:node, ref)
    case Config.get(:backup_mode, "sync") do
      "sync" -> sync_snapshot(pid, role, context, node_idx, state)
      "async" -> spawn(fn -> __MODULE__.sync_snapshot(pid, role, context, node_idx, state) end)
      "masterslave" -> masterslave_snapshot(pid, role, context, node_idx, state)
      mode -> raise "unknown backup_mode `#{mode}`, expected one of [sync,async,masterslave]"
    end
  end

  # def replicate_racksplit({nodes_count, replica_count}, pid, node_idx) do
  #   id = Murmur.hash_x86_128(pid)
  #   0..(replica_count - 1)
  #   |> Enum.map(fn r -> 
  #     idx = rem(id + r*div(nodes_count,replica_count), nodes_count-1)
  #     if idx >= node_idx, do: idx+1, else: idx
  #   end)

  def replicate_range({1, _}, _, _) do
    MapSet.new
  end
  
  def replicate_range({nodes_count, replica_count}, pid, node_idx) do
    id = Murmur.hash_x86_128(pid)
    0..(replica_count - 1)
    |> Enum.map(fn r -> 
      idx = rem(id + r, nodes_count-1)
      if idx >= node_idx, do: idx+1, else: idx 
    end)	
    |> MapSet.new
  end

  def masterslave_snapshot(pid, role, context = %Context{_skr: {ref,_}, _epoch: 1}, node_idx, state) do   
    admin_cast(NodeStore.get(:failure_stores, ref, node_idx), {:masterslave, node_idx, ref, {pid, role, RT.node_name_for_context(context), 1, state, context.strategy_dag}})
  end

  def masterslave_snapshot(pid, role, context = %Context{_skr: {ref,_}}, node_idx, state) do   
    admin_cast(NodeStore.get(:failure_stores, ref, node_idx), {:masterslave, node_idx, ref, {pid, role, RT.node_name_for_context(context), context._epoch, state}})
  end

  def sync_snapshot(pid, role, context = %Context{_skr: {ref,_}, _epoch: 1}, node_idx, state) do 
    replica_ids_set = replicate_range(ConstantStore.get(:replica_count, ref), pid, node_idx)
    NodeStore.get_all(:failure_stores, ref)
      |> Enum.with_index
      |> Enum.each(fn {node, idx} -> 
        if MapSet.member?(replica_ids_set, idx) do
          admin_cast(node, {:snapshot, {pid, role, RT.node_name_for_context(context), 1, state, context.strategy_dag}})
        else
          admin_cast(node, {:snapshot, {RT.node_name_for_context(context), 1, context.strategy_dag}})
        end
      end)
  end

  def sync_snapshot(pid, role, context = %Context{_skr: {ref,_}}, node_idx, state) do 
    replica_ids_set = replicate_range(ConstantStore.get(:replica_count, ref), pid, node_idx)
    NodeStore.get_all(:failure_stores, ref)
      |> Enum.with_index
      |> Enum.each(fn {node, idx} -> 
        if MapSet.member?(replica_ids_set, idx) do 
          admin_cast(node, {:snapshot, {pid, role, RT.node_name_for_context(context), context._epoch, state}})
        else 
          admin_cast(node, {:snapshot, {RT.node_name_for_context(context), context._epoch}}) 
        end
      end)
  end
  
  def fetch_refs(depl_ref, fetcher), do: admin_broadcast(depl_ref, {:fetch_refs, fetcher})

  def fetch_backup(context, %__MODULE__.Reference{worker_pid: ref, store_pid: store_pid}, role), do: admin_cast(store_pid, {:fetch_backup, ref, role, RT.node_name_for_context(context), self()})

  # count total PIDs
  def deployment_in_id(strategy_dag) do
    Enum.reduce(strategy_dag.nodes, 0, fn 
      {_k, node}, acc -> acc + (node.pids |> Enum.count)
    end)
  end

  # first epoch; on end condition => pass to next epoch without dropping 
  def check_drop_epoch(
    _,
    { 0, snapshot_dict, %MapSet{map: undeployed_ops}, epoch_recv_count, epoch_max_count} = data
  ) do 
    if undeployed_ops==%{} and Map.get(epoch_recv_count,1)==0 do
      { 1, 
        snapshot_dict, 
        epoch_recv_count 
          |> Map.delete(1) 
          |> Map.to_list 
          |> Enum.map(fn {k,v} -> {k,v+epoch_max_count} end) # update counter of all next counters now that we know the total count we expect
          |> Map.new, 
        epoch_max_count
      }
    else
      data
    end
  end

  # nonfirst epoch; on end condition => dropping of all queues bellow given tick on his local backup dict
  def check_drop_epoch(
    epoch_tick,
    {_, snapshot_dict, epoch_recv_count, epoch_max_count} = data
  ) do 
    if Map.get(epoch_recv_count,epoch_tick)==0 do
      { epoch_tick, 
        drop_epoch(snapshot_dict, epoch_tick), 
        Map.filter(epoch_recv_count, fn {k,_} -> k>epoch_tick end), 
        epoch_max_count
      }
    else
      data
    end
  end

  def add_backup(snapshot_dict, {pid, role, operation_n}, 1, snapshot_state) do 
    MapMacro.update(snapshot_dict, [operation_n, role, pid], :queue.in({1, snapshot_state}, :queue.new), 
      fn q -> :queue.in({1, snapshot_state}, q) 
    end)
  end

  def add_backup(snapshot_dict, {pid, role, operation_n}, epoch_tick, snapshot_state) do 
    MapMacro.update(snapshot_dict, [operation_n, role, pid], :queue.in({epoch_tick, snapshot_state}, :queue.new), 
      fn q -> 
        {{:value, {_, last_entry}}, q_rest} = :queue.out_r(q)
        :queue.in({epoch_tick, snapshot_state}, (if last_entry==snapshot_state, do: q_rest, else: q))
      end)
  end

  def drop_epoch(snapshot_dict, epoch_tick) do
    MapMacro.get_and_update(snapshot_dict, &drop_while(&1, epoch_tick - 1))
  end

  def drop_while(q, epoch) do 
    cond do
      :queue.is_empty(q) -> :pop
      elem(:queue.get(q), 0) <= epoch -> drop_while(:queue.drop(q), epoch)
      true -> q 
    end
  end

  def update_undeployed_ops({operation_n, deployment}, {0, snapshot_dict, undeployed_ops, epoch_recv_count, epoch_max_count}) do 
    if MapSet.member?(undeployed_ops, operation_n) do
      new_undeployed_ops = 
        MapSet.delete(undeployed_ops, operation_n)
      pid_count = 
        deployment_in_id(deployment)
      new_epoch_max_count = 
        epoch_max_count + pid_count
      new_epoch_recv_count = 
        Map.update(epoch_recv_count, 1, pid_count - 1, fn count -> count + pid_count - 1 end)
      {0, snapshot_dict, new_undeployed_ops, new_epoch_recv_count, new_epoch_max_count}
    else
      new_epoch_recv_count = 
        Map.update!(epoch_recv_count, 1, fn count -> count - 1 end)
      {0, snapshot_dict, undeployed_ops, new_epoch_recv_count, epoch_max_count}
    end
  end
  
  def new_snapshot(
    {pid, role, operation_n, epoch_tick, snapshot_state, deployment},
    {0, snapshot_dict, undeployed_ops, epoch_recv_count, epoch_max_count}
  ) do 
    if MapSet.member?(undeployed_ops, operation_n) do
      new_undeployed_ops = 
        MapSet.delete(undeployed_ops, operation_n)
      pid_count = 
        deployment_in_id(deployment)
      new_epoch_max_count = 
        epoch_max_count + pid_count
      new_epoch_recv_count = 
        if pid_count==0, do: epoch_recv_count, else: Map.update(epoch_recv_count, epoch_tick, pid_count - 1, fn count -> count + pid_count - 1 end)
      new_snapshot_dict = 
        add_backup(snapshot_dict, {pid, role, operation_n}, epoch_tick, snapshot_state)
      {0, new_snapshot_dict, new_undeployed_ops, new_epoch_recv_count, new_epoch_max_count}

    else
      new_epoch_recv_count = 
        Map.update(epoch_recv_count, epoch_tick, -1, fn count -> count - 1 end)
      new_snapshot_dict = 
        add_backup(snapshot_dict, {pid, role, operation_n}, epoch_tick, snapshot_state)

      {0, new_snapshot_dict, undeployed_ops, new_epoch_recv_count, epoch_max_count}
    end
  end

  def new_snapshot(
    {pid, role, operation_n, epoch_tick, snapshot_state},
    {lowest_epoch, snapshot_dict, epoch_recv_count, epoch_max_count}
  ) do 
    new_epoch_recv_count = 
      Map.update(epoch_recv_count, epoch_tick, epoch_max_count - 1, fn count -> count - 1 end)
    new_snapshot_dict = 
      add_backup(snapshot_dict, {pid, role, operation_n}, epoch_tick, snapshot_state)

    {lowest_epoch, new_snapshot_dict, new_epoch_recv_count, epoch_max_count}
  end

  def update_counter(
    epoch_tick,
    {lowest_epoch, snapshot_dict, epoch_recv_count, epoch_max_count}
  ) do 
    new_epoch_recv_count = 
      Map.update(epoch_recv_count, epoch_tick, epoch_max_count - 1, fn count -> count - 1 end)

    {lowest_epoch, snapshot_dict, new_epoch_recv_count, epoch_max_count}
  end

  def handle_cast({:snapshot, {pid, role, operation_n, 1, snapshot_state, deployment}}, state) do
    {:noreply, check_drop_epoch(
      1,
      new_snapshot({pid, role, operation_n, 1, snapshot_state, deployment}, state)
    )}
  end

  def handle_cast({:snapshot, {operation_n, 1, deployment}}, state) do
    {:noreply, check_drop_epoch(
      1,
      update_undeployed_ops({operation_n, deployment}, state)
    )}
  end

  def handle_cast({:snapshot, {pid, role, operation_n, epoch_tick, snapshot_state}}, state) do
    {:noreply, check_drop_epoch(
      epoch_tick,
      new_snapshot({pid, role, operation_n, epoch_tick, snapshot_state}, state)
    )}
  end

  def handle_cast({:snapshot, {_operation_n, epoch}}, state) do
    {:noreply, check_drop_epoch(
      epoch,
      update_counter(epoch, state)
    )}
  end

  def handle_cast({:masterslave, node_idx, ref, {pid, role, opn, 1, backup_state, dag}}, state) do
    replica_ids_set = replicate_range(ConstantStore.get(:replica_count, ref), pid, node_idx)
    NodeStore.get_all(:failure_stores, ref)
      |> Enum.with_index
      |> Enum.each(fn {node, idx} -> 
        cond do 
          node == self() -> nil
          MapSet.member?(replica_ids_set, idx) -> admin_cast(node, {:snapshot, {pid, role, opn, 1, backup_state, dag}})
          true -> admin_cast(node, {:snapshot, {opn, 1, dag}})
        end
      end)
    handle_cast({:snapshot, {pid, role, opn, 1, backup_state, dag}}, state)
  end

  def handle_cast({:masterslave, node_idx, ref, {pid, role, opn, epoch, backup_state}}, state) do
    replica_ids_set = replicate_range(ConstantStore.get(:replica_count, ref), pid, node_idx)
    NodeStore.get_all(:failure_stores, ref)
      |> Enum.with_index
      |> Enum.each(fn {node, idx} -> 
        cond do 
          node == self() -> nil
          MapSet.member?(replica_ids_set, idx) -> admin_cast(node, {:snapshot, {pid, role, opn, epoch, backup_state}})
          true -> admin_cast(node, {:snapshot, {opn, epoch}})
        end
      end)
    handle_cast({:snapshot, {pid, role, opn, epoch, backup_state}}, state)
  end

  def handle_cast(
    {:fetch_refs, fetcher}, 
    {_, snapshot_dict, _, _} = data
  ) do
    send(fetcher, {:backup_ref, MapMacro.map(snapshot_dict, fn _ -> self() end)})
    {:noreply, data}
  end

  def handle_cast(
    {:fetch_backup, ref, role, operation_n, fetcher}, 
    {lowest_epoch, snapshot_dict, _epoch_recv_count, epoch_max_count}
  ) do
    {_, backup_value} = MapMacro.get(snapshot_dict, [operation_n,role,ref]) 
      |> drop_while(lowest_epoch-1) 
      |> :queue.get()
      RT.Worker.send_backup(fetcher, {lowest_epoch, backup_value})

    new_snapshot_dict = MapMacro.update!(snapshot_dict, [operation_n, role], fn mp_role -> 
      {q, map} = Map.pop!(mp_role, ref)
      Map.put(map, fetcher, :queue.in(q |> drop_while(lowest_epoch-1) |> :queue.get(), :queue.new))
    end)
    
    {:noreply, {lowest_epoch, new_snapshot_dict, %{}, epoch_max_count}}
  end
end