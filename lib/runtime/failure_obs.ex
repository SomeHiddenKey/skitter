use Skitter

defmodule Skitter.Runtime.FailureObs do
  use GenServer
  require Logger
  alias Skitter.Mode.Master.WorkerConnection, as: MWC 
  alias Skitter.Runtime, as: RT 
  alias Skitter.Runtime.{
    ConstantStore,
    NodeStore,
    FailureBackupStore,
    WorkflowWorkerSupervisor
  }
  require Skitter.Runtime.{
    ConstantStore,
    NodeStore
  }
  require MapMacro
  @snapshot_nodes Application.compile_env(:skitter, :ackers, 1)

  def start_link(arg) do
    GenServer.start_link(__MODULE__, [arg[:deployment]], name: arg[:name])
  end

  def init([deployment_ref]) do
    dbg :STARTED
    unless RT.mode() == :local, do: MWC.subscribe_down()
    {:ok, {:uninitialized, deployment_ref, %{}}}
  end

  def put_worker_pid(ref, idx, role, pid), do: GenServer.cast(ConstantStore.get(:failure_obs, ref), {:put_pid, idx, role, pid})

  def notify_everywhere(ref) do 
    GenServer.cast(ConstantStore.get(:failure_obs, ref), {:put_pid_everywhere, ref})
  end
  
  def handle_cast({:put_pid, idx, role, pid}, {:uninitialized, deployment_ref, pid_map}) do
    new_pid_map = MapMacro.update(pid_map, [idx, role], [pid], &[pid|&1])
    {:noreply, {:uninitialized, deployment_ref, new_pid_map}}
  end

  def handle_cast({:put_pid_everywhere, ref}, {:uninitialized, deployment_ref, pid_map}) do
    pid_list = pid_map |> Map.to_list() |> Enum.sort_by(&elem(&1, 0)) |> Enum.map(&elem(&1, 1))
    NodeStore.put_everywhere(pid_list, :pid_store, deployment_ref)
    RT.notify_workers(ref)
    {:noreply, deployment_ref}
  end

  def handle_info({:worker_down, worker}, deployment_ref) do
    dbg {:DOWN, worker}
    nodes = ConstantStore.get(:wf_nodes, deployment_ref)
    Skitter.stop(deployment_ref)
    unless RT.mode() == :local, do: MWC.unsubscribe_down()
    FailureBackupStore.fetch_refs(deployment_ref, self())
    {:noreply, {%{}, nodes, @snapshot_nodes - 1}}
  end

  def handle_info({:backup_ref, new_refs}, {refs_so_far, nodes, 0}) do
    backup_refs = MapMacro.merge(new_refs, refs_so_far)
    deployment_ref = Skitter.Runtime.redeploy(nodes, backup_refs)
    unless RT.mode() == :local, do: MWC.subscribe_down()
    dbg {refs_so_far,new_refs,backup_refs}
    {:noreply, deployment_ref}
  end 

  def handle_info({:backup_ref, new_refs}, {refs_so_far, nodes, count_so_far}) do
    {:noreply, {MapMacro.merge(new_refs, refs_so_far), nodes, count_so_far-1}}
  end 
end