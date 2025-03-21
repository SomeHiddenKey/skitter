use Skitter

defmodule Skitter.Runtime.BackupStore.Reference do

  defstruct [:store_pid, :worker_pid]
  def %__MODULE__{worker_pid: w1} == %__MODULE__{worker_pid: w2}, do: Kernel.==(w1, w2)

  def round_robin(refs), do: round_robin(refs, Skitter.Remote.Registry.workers())

  def round_robin(_, []) do
    exit("can't restart from backup: not enough workers available")
  end

  def round_robin(state_refs, remotes) do
    remote_count = Enum.count(remotes)
    state_refs_count = Enum.count(state_refs)
    base_count = div(state_refs_count, remote_count)
    flip_index = Integer.mod(state_refs_count, remote_count)
    state_refs
    |> round_robin(base_count, flip_index, 0, 0, [])
    |> Enum.zip(remotes)
    |> Enum.map(fn {refs, remote} -> {remote, refs} end)
  end

  defp round_robin([], _, _, _, _, trt), do: trt
  defp round_robin(states, base_count, flip_index, b_i, 0, trt) do 
    counter = if b_i<flip_index, do: base_count+1, else: base_count
    round_robin(states, base_count, flip_index, b_i+1, counter, [[]|trt])
  end
  defp round_robin([sh|st], base_count, flip_index, b_i, s_i, [th|tt]) do 
    round_robin(st, base_count, flip_index, b_i, s_i-1, [[sh|th]|tt])
  end
end

