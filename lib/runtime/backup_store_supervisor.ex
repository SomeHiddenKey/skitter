# Copyright 2018 - 2022, Mathijs Saey, Vrije Universiteit Brussel

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0

defmodule Skitter.Runtime.BackupStoreSupervisor do
  use DynamicSupervisor
  alias Skitter.Runtime.{BackupStore,FailureObs,ConstantStore}
  require ConstantStore

  def start_link(arg), do: DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @impl true
  def init(_arg), do: DynamicSupervisor.init(strategy: :one_for_one)

  def spawn_store(ref, nodes) do
    {:ok, pid} =
      DynamicSupervisor.start_child(
        __MODULE__,
        {BackupStore, {ref, nodes}}
      )
  end

  def spawn_obs(ref) do
    {:ok, pid} =
      DynamicSupervisor.start_child(
        __MODULE__,
        {FailureObs, ref}
      )

    ConstantStore.put(pid, :node_worker_supervisor, ref)
  end
end
