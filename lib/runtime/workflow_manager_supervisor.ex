# Copyright 2018 - 2022, Mathijs Saey, Vrije Universiteit Brussel

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0

defmodule Skitter.Runtime.WorkflowManagerSupervisor do
  @snapshot_nodes Application.compile_env(:skitter, :ackers, 1)
  @snapshot_replicas Application.compile_env(:skitter, :replicas, 1)
  @moduledoc false
  # Supervisor which supervises workflow managers.

  use DynamicSupervisor
  alias Skitter.Runtime.{
    WorkflowManager,
    FailureBackupStore,
    FailureObs
  }

  def start_link(arg), do: DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @impl true
  def init(_arg), do: DynamicSupervisor.init(strategy: :one_for_one)

  def add_manager(ref) do
    DynamicSupervisor.start_child(__MODULE__, {WorkflowManager, ref})
  end

  def add_backup_server(ref, nodes) do
    if @snapshot_replicas > @snapshot_nodes, do: raise "replica count can't be higher than node count"

    children = 0..(@snapshot_nodes - 1) |> Enum.map(fn i ->  Supervisor.child_spec({
      FailureBackupStore, 
      name: :"#{FailureBackupStore}.#{i}",
      nodes: nodes |> MapSet.new(&elem(&1, 0))
    }, id: {FailureBackupStore, i}) end)

    obs = Supervisor.child_spec({
      FailureObs, 
      name: :"#{FailureObs}",
      deployment: ref
    }, id: FailureObs)

    # {:ok, supervisor_pid} = DynamicSupervisor.start_link(__MODULE__, )
    [obs|children] 
      |> Enum.map(&DynamicSupervisor.start_child(__MODULE__, &1))
      |> Enum.map(fn 
        {:ok,pid} -> pid
        {:error, err} -> raise err
      end)
  end

  def spawned_workflow_references do
    __MODULE__
    |> DynamicSupervisor.which_children()
    |> Enum.map(fn {_, pid, _, _} -> WorkflowManager.ref(pid) end)
  end
end
