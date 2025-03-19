# Copyright 2018 - 2022, Mathijs Saey, Vrije Universiteit Brussel

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

defmodule Skitter.Runtime.Emit do
  @moduledoc false
  alias Skitter.Runtime.NodeStore
  alias Skitter.Strategy.Context
  alias Skitter.Token
  alias Skitter.Runtime.Worker

  require NodeStore
  use Skitter.Telemetry

  def emit(%Context{_skr: {:deploy, _, _}}, _, _) do
    raise(Skitter.DefinitionError, "Attempted to emit data inside a deploy hook")
  end

  def emit(ctx = %Context{_skr: {ref, idx}}, emit) do
    Telemetry.emit([:runtime, :emit], %{}, %{context: ctx, emit: emit})
    node_links = NodeStore.get(:links, ref, idx)
    Enum.each(emit, fn {out_port, enum} -> enum(ctx, enum, Map.fetch(node_links, out_port), &token/3) end)
  end

  def emit_epoch(ctx = %Context{_skr: {ref, idx}, operation: operation}, {e, c}) do
    emit = operation |> Skitter.Operation.out_ports() |> Enum.map(&{&1, [{e, c}]})
    node_links = NodeStore.get(:links, ref, idx)
    Enum.each(emit, fn {out_port, enum} -> enum(ctx, enum, Map.fetch(node_links, out_port), &epoch/3) end)
  end

  defp enum(_, _, :error, _), do: :ok
  defp enum(_, _, {:ok, []}, _), do: :ok
  defp enum(ctx, lst, {:ok, dsts}, f) when is_list(lst), do: Enum.each(lst, &f.(ctx, dsts, &1))
  defp enum(ctx, enum, {:ok, dsts}, f), do: Stream.each(enum, &f.(ctx, dsts, &1)) |> Stream.run()

  defp token(outer_ctx, dsts, tkn = %Token{}) do
    Enum.each(dsts, fn {ctx, prt} ->
      tkn = %{tkn | port: prt}

      Telemetry.wrap [:hook, :deliver], %{pid: self(), context: ctx, token: tkn} do
        ctx.strategy.deliver(put_in(ctx._epoch, outer_ctx._epoch), tkn)
      end
    end)
  end

  defp token(ctx, dsts, val), do: token(ctx, dsts, %Token{value: val})

  defp epoch(outer_ctx, dsts, {e, c}) do
    Enum.each(dsts, fn {%Context{_skr: {ref, idx}}, prt} ->
      Worker.deliver_epoch(NodeStore.get(:dag, ref, idx) , {prt, e, c})
    end)
  end
end
