use Skitter

defmodule Skitter.Runtime.FailureObs do
  use GenServer
  require Logger
  alias Skitter.Mode.Master.WorkerConnection, as: MWC 
  alias Skitter.Strategy
  alias Skitter.Runtime, as: RT 
  alias Skitter.Remote
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
  @snapshot_replicas Application.compile_env(:skitter, :backup_replicas, 1)
  @backup_mode Application.compile_env(:skitter, :backup_mode, "sync")

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg)
  end

  def init(deployment_ref) do
    ConstantStore.put_everywhere(self(), :failure_obs, deployment_ref)
    unless RT.mode() == :local, do: MWC.subscribe_down()
    Logger.info("Failure-Observer started", [deployment: deployment_ref])
    {:ok, {:uninitialized, deployment_ref, %{}, []}}
  end

  def put_worker_pid(ref, idx, role, pid), do: GenServer.cast(ConstantStore.get(:failure_obs, ref), {:put_worker_pid, idx, role, pid})

  def put_store_pid(ref, pid, node_id), do: GenServer.cast(ConstantStore.get(:failure_obs, ref), {:put_store_pid, pid, node_id})

  def notify_everywhere(ref) do 
    GenServer.cast(ConstantStore.get(:failure_obs, ref), :put_pid_everywhere)
  end

  defp redeploy(worker_refs, nodes, old_deploy_ref) do 
    backup_refs = MapMacro.map(worker_refs, 2, &Enum.map(&1, fn {ref, store} -> %BackupStore.Reference{worker_pid: ref, store_pid: store} end))

    deployment_ref = Skitter.Runtime.redeploy(nodes, old_deploy_ref, backup_refs)
    # unless RT.mode() == :local, do: MWC.subscribe_down()
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
      replica_count = max(1, min(backup_stores_count-1,@snapshot_replicas))
      NodeStore.put_everywhere(Enum.map(store_list, &elem(&1, 1)), :failure_stores, ref)
      ConstantStore.put_everywhere({backup_stores_count, replica_count}, :replica_count, ref)
      Logger.info("Backup Store & Worker information updated everywhere\n> Stores:   #{backup_stores_count}\n> Replicas: #{replica_count}\n> Mode:     #{@backup_mode}", [deployment: ref])
      Remote.on_all_workers(fn -> ConstantStore.put(Enum.find_index(rems, &(Node.self() == &1)), :node, ref) end)
    end

    NodeStore.get_all(:dag, ref) 
    |> Enum.zip(pid_list)
    |> Enum.map(fn {{ctx, dag}, pid_ctx} -> Strategy.DAG.build(ctx, dag, pid_ctx) end) 
    |> NodeStore.put_everywhere(:dag, ref)

    RT.notify_workers(ref)
    {:noreply, {:running, ref, MapMacro.reduce(pid_map, 0, fn x, acc -> acc + Enum.count(x) end)}}
  end

  def handle_info({:worker_down, worker}, {:uninitialized, deployment_ref, _pid_map, _backup_stores}) do
    Logger.alert("Remote disconnected", [remote: worker, deployment: deployment_ref])
    nodes = ConstantStore.get(:wf_nodes, deployment_ref)
    RT.stop(deployment_ref)
    Skitter.Runtime.redeploy(nodes, deployment_ref)
    Logger.info("Workflow redeployed from latest checkpoint", [deployment: deployment_ref])
    {:noreply, {:uninitialized, deployment_ref, %{}, []}}
  end
  
  def handle_info({:worker_down, worker}, {:redeploying, refs_so_far, nodes, old_deploy_ref, count}) do
    Logger.alert("Remote disconnected", [remote: worker, deployment: old_deploy_ref])
    if refs_so_far != %{}, do: BackupStore.fetch_refs(old_deploy_ref, self())
    {:noreply, {:redeploying, %{}, nodes, old_deploy_ref, count}}
  end # consecutive workers down

  def handle_info({:worker_down, worker}, {:running, deployment_ref, count}) do
    Logger.alert("Remote disconnected", [remote: worker, deployment: deployment_ref])
    nodes = ConstantStore.get(:wf_nodes, deployment_ref)
    RT.stop_nonfailure_inst(deployment_ref)
    # unless RT.mode() == :local, do: MWC.unsubscribe_down()
    BackupStore.fetch_refs(deployment_ref, self())
    {:noreply, {:redeploying, %{}, nodes, deployment_ref, count}}
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