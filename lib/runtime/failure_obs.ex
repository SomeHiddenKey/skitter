use Skitter

defmodule Skitter.Runtime.FailureObs do
  use GenServer
  require Logger
  require Keyed.FailureSafeStrategy
  alias Skitter.Mode.Master.WorkerConnection, as: MWC 
  alias Skitter.Runtime, as: RT 
  @snapshot_nodes Application.compile_env!(:word_count, :ackers)

  def start_link(arg) do
    GenServer.start_link(__MODULE__, [arg[:workflow]], name: arg[:name])
  end

  def init([workflow]) do
    deployment_ref = Skitter.deploy(workflow)
    unless RT.mode() == :local, do: MWC.subscribe_down()
    {:ok, {workflow, deployment_ref}}
  end

  def handle_info({:worker_down, worker}, {workflow, deployment_ref}) do
    dbg {:DOWN, worker}
    Skitter.stop(deployment_ref)
    unless RT.mode() == :local, do: MWC.unsubscribe_down()
    FailureAdmin.fetch_refs(self())
    {:noreply, {workflow, %{}, @snapshot_nodes - 1}}
  end

  def handle_info({:backup_ref, new_refs}, {workflow, refs_so_far, 0}) do
    backup_refs = MapMacro.merge(new_refs, refs_so_far)
    # FailureAdmin.dump()
    deployment_ref = Skitter.Runtime.redeploy(workflow, backup_refs)
    unless RT.mode() == :local, do: MWC.subscribe_down()
    dbg {refs_so_far,new_refs,backup_refs}
    {:noreply, {workflow, deployment_ref}}
  end 

  def handle_info({:backup_ref, new_refs}, {workflow, refs_so_far, count_so_far}) do
    dbg {refs_so_far,new_refs,MapMacro.merge(new_refs, refs_so_far)}
    {:noreply, {workflow, MapMacro.merge(new_refs, refs_so_far), count_so_far-1}}
  end 
end