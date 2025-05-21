use Skitter

defmodule Skitter.Runtime.FailureObs do
  use GenServer
  require Logger
  alias Skitter.Mode.Master.WorkerConnection, as: MWC 
  alias Skitter.{Runtime, Remote, Config, Strategy}
  alias Skitter.Runtime.{
    ConstantStore,
    NodeStore,
    BackupStore
  }
  require Skitter.Runtime.{
    ConstantStore,
    NodeStore
  }
  require MapMacro

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg)
  end

  def init(deployment_ref) do
    ConstantStore.put_everywhere(self(), :failure_obs, deployment_ref)
    unless Runtime.mode() == :local, do: MWC.subscribe_down()
    Logger.info("Failure-Observer started", [deployment: deployment_ref])
    {:ok, {:uninitialized, deployment_ref, %{}, []}}
  end

  def put_worker_pid(ref, idx, role, pid), do: GenServer.cast(ConstantStore.get(:failure_obs, ref), {:put_worker_pid, idx, role, pid})

  def put_store_pid(ref, pid, node_id), do: GenServer.cast(ConstantStore.get(:failure_obs, ref), {:put_store_pid, pid, node_id})
  
  def notify_epoch_drop(ref, epoch), do: GenServer.cast(ConstantStore.get(:failure_obs, ref), {:notify_epoch_drop, epoch})

  def notify_everywhere(ref) do 
    GenServer.cast(ConstantStore.get(:failure_obs, ref), :put_pid_everywhere)
  end

  defp redeploy(worker_refs, nodes, old_deploy_ref) do 
    backup_refs = MapMacro.map(worker_refs, 2, &Enum.map(&1, fn {ref, store} -> %BackupStore.Reference{worker_pid: ref, store_pid: store} end))

    deployment_ref = Skitter.Runtime.redeploy(nodes, old_deploy_ref, backup_refs)
    Logger.info("Workflow redeployed from latest checkpoint", [deployment: deployment_ref])
    {:noreply, {:uninitialized, deployment_ref, %{}, nil}}
  end
  
  def handle_cast({:put_store_pid, pid, node_id}, {:uninitialized, deployment_ref, pid_map, backup_stores}) do
    {:noreply, {:uninitialized, deployment_ref, pid_map, [{node_id, pid}|backup_stores]}}
  end

  def handle_cast({:put_worker_pid, idx, role, pid}, {:uninitialized, deployment_ref, pid_map, backup_stores}) do
    new_pid_map = MapMacro.update(pid_map, [idx, role], [pid], &[pid|&1])
    {:noreply, {:uninitialized, deployment_ref, new_pid_map, backup_stores}}
  end

  def handle_cast(:put_pid_everywhere, {:uninitialized, ref, pid_map, backup_stores}) do
    pid_list = pid_map 
    |> Map.to_list() 
    |> Enum.sort_by(&elem(&1, 0)) 
    |> Enum.map(&elem(&1, 1))
    
    if backup_stores != nil do 
      rems = Remote.workers()
      store_list = Enum.sort_by(backup_stores, fn {node_id, _} -> Enum.find_index(rems, &(node_id == &1)) end)
      backup_stores_count = Enum.count(store_list)
      replica_count = max(1, min(backup_stores_count-1,Config.get(:backup_replicas, 1)))
      NodeStore.put_everywhere(Enum.map(store_list, &elem(&1, 1)), :failure_stores, ref)
      ConstantStore.put_everywhere({backup_stores_count, replica_count}, :replica_count, ref)
      Logger.info("Backup Store & Worker information updated everywhere\n> Stores:   #{backup_stores_count}\n> Replicas: #{replica_count}\n> Mode:     #{Config.get(:backup_mode, "sync")}", [deployment: ref])
      Remote.on_all_workers(fn -> ConstantStore.put(Enum.find_index(rems, &(Node.self() == &1)), :node, ref) end)
    end

    NodeStore.get_all(:dag, ref) 
    |> Enum.zip(pid_list)
    |> Enum.map(fn {{ctx, dag}, pid_ctx} -> Strategy.DAG.build(ctx, dag, pid_ctx) end) 
    |> NodeStore.put_everywhere(:dag, ref)

    Runtime.notify_workers(ref)
    {:noreply, {:running, Map.new(), ref, MapMacro.reduce(pid_map, 0, fn x, acc -> acc + Enum.count(x) end)}}
  end

  def handle_cast({:notify_epoch_drop, epoch}, {:running, epoch_map, deployment_ref, count}) do 
    if Map.get(epoch_map, epoch, 0)==1 do 
      BackupStore.admin_broadcast(deployment_ref, {:notify_epoch_drop, epoch})
      new_epoch_map = Map.delete(epoch_map, epoch)
      {:noreply, {:running, new_epoch_map, deployment_ref, count}}
    else 
      new_epoch_map = Map.update(epoch_map, epoch, Enum.count(Remote.workers()) - 1, fn count -> count - 1 end)
      {:noreply, {:running, new_epoch_map, deployment_ref, count}}
    end
  end
  
  def handle_cast({:notify_epoch_drop, _}, state), do: {:noreply, state}

  def handle_info({:worker_down, worker}, {:uninitialized, deployment_ref, _pid_map, _backup_stores}) do
    Logger.alert("Remote disconnected", [remote: worker, deployment: deployment_ref])
    nodes = ConstantStore.get(:wf_nodes, deployment_ref)
    Runtime.stop(deployment_ref)
    Skitter.Runtime.redeploy(nodes, deployment_ref)
    Logger.info("Workflow redeployed from latest checkpoint", [deployment: deployment_ref])
    {:noreply, {:uninitialized, deployment_ref, %{}, []}}
  end
  
  def handle_info({:worker_down, worker}, {:redeploying, refs_so_far, nodes, old_deploy_ref, count}) do
    Logger.alert("Remote disconnected", [remote: worker, deployment: old_deploy_ref])
    if refs_so_far != %{}, do: BackupStore.fetch_refs(old_deploy_ref, self())
    {:noreply, {:redeploying, %{}, nodes, old_deploy_ref, count}}
  end # consecutive workers down

    def handle_info({:worker_down, worker}, {:running, epoch_map, deployment_ref, count}) do
    Logger.alert("Remote disconnected", [remote: worker, deployment: deployment_ref])
    if Map.has_key?(epoch_map, 0) do
      nodes = ConstantStore.get(:wf_nodes, deployment_ref)
      Runtime.stop(deployment_ref)
      Skitter.Runtime.redeploy(nodes, deployment_ref)
      Logger.info("Workflow redeployed from start", [deployment: deployment_ref])
      {:noreply, {:uninitialized, deployment_ref, %{}, []}}
    else
      nodes = ConstantStore.get(:wf_nodes, deployment_ref)
      Runtime.stop_nonfailure_inst(deployment_ref)
      BackupStore.fetch_refs(deployment_ref, self())
      {:noreply, {:redeploying, %{}, nodes, deployment_ref, count}}
    end
  end

  def handle_info({:backup_ref, new_refs}, {:redeploying, refs_so_far, nodes, deployment_ref, expected_worker_count}) do
    new_refs_so_far = MapMacro.merge(new_refs, refs_so_far)

    if MapMacro.count(new_refs_so_far) < expected_worker_count do
      {:noreply, {:redeploying, new_refs_so_far, nodes, deployment_ref, expected_worker_count}}
    else
      redeploy(new_refs_so_far, nodes, deployment_ref)
    end
  end 

  def handle_info({:backup_ref, _}, state), do: {:noreply, state} 
end