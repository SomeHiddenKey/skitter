# Copyright 2018 - 2022, Mathijs Saey, Vrije Universiteit Brussel

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

defmodule Skitter.Strategy do
  @moduledoc """
  Strategy type definition and utilities.

  A strategy is a reusable piece of logic which determines how an operation is distributed at
  runtime. It is defined as a collection of _hooks_: functions which each define an aspect of the
  distributed behaviour of an operation.

  An operation strategy is defined as an elixir module which implements the
  `Skitter.Strategy.Operation` behaviour. It is recommended to define a strategy using
  `Skitter.DSL.Strategy.defstrategy/3`.

  This module defines the strategy and context types.
  """
  alias Skitter.Operation

  @typedoc """
  A strategy is defined as a module.
  """
  @type t :: module()

  @typedoc """
  Immutable data of a data processing pipeline.

  A strategy which is deployed over the cluster has access to an immutable set of data which is
  termed the _deployment_. A strategy can specify which data to store in its deployment inside the
  `c:Skitter.Strategy.Operation.deploy/2` hook. Afterwards, the other strategy hooks have access
  to the data stored within the deployment.

  Note that an operation strategy can only access its own deployment data.
  """
  @type deployment :: any()

  @typedoc """
  Context information for strategy hooks.

  A strategy hook often needs information about the context in which it is being called. Relevant
  information about the context is stored inside the context, which is passed as the first
  argument to every hook.

  The following information is stored:

  - `operation`: The operation for which the hook is called.
  - `strategy`: The strategy of the operation.
  - `deployment`: The current deployment data. `nil` if the deployment is not created yet (e.g. in
  `deploy`)
  - `_skr`: Data stored by the runtime system. This data should not be accessed or modified.
  """
  @type context :: %__MODULE__.Context{
          operation: Operation.t(),
          strategy: t(),
          deployment: deployment() | nil,
          strategy_dag: dag() | nil,
          _epoch: number() | nil,
          _skr: any()
        }

  @type dag :: %__MODULE__.DAG{
          in: [any()],
          in_count: non_neg_integer(),
          out: [any()],
          out_count: non_neg_integer(),
          nodes: map()
        }

  @type dag_node :: %__MODULE__.DAG.Node{
          in: [any()],
          out: [any()],
          pids: [pid()],
          pids_len: non_neg_integer()
        }

  defmodule Context do
    @moduledoc false
    @derive {Inspect, except: [:_skr, :deployment]}
    defstruct [:operation, :strategy, :deployment, :strategy_dag, :_epoch, :_skr]
  end

  defmodule DAG.Node do
    @moduledoc false
    defstruct [:in, :out, :pids, :pids_len]
  end

  defmodule DAG do
    @moduledoc false
    defstruct [:in, :in_count, :out, :out_count, :nodes]

    defp in_roles(ctx, graph, role) do 
      List.flatten(for {from,to} <- graph, Enum.member?(to, role), into: [] do
        case from do
          {:in} -> Skitter.Operation.in_ports(ctx.operation())
          {:out} -> throw "Out role can't refer to back to another node: Breaks DAG" 
          _ -> [{:inner, from}]
        end
      end)
    end

    defp to_node(ctx, graph, {role, pids}) do
      %__MODULE__.Node{
        in: in_roles(ctx, graph, role),
        out: Map.get(graph,role,[]),
        pids: pids,
        pids_len: length(pids)
      }
    end

    def build(ctx, graph, pid_context) do
      dbg {graph, pid_context}

      inv_graph = pid_context 
        |> Enum.map(fn {role, pids} -> {role, to_node(ctx, graph, {role,pids})} end)
        |> Map.new()

      in_roles = Map.get(graph,{:in},[])
      out = Enum.filter(inv_graph, fn {_k, %__MODULE__.Node{out: out}} -> {:out} in out end)

      %__MODULE__{
        in: in_roles,
        in_count: length(in_roles),
        out: Enum.map(out, &elem(&1, 0)),
        out_count: Enum.reduce(out, 0, fn {_k, node}, acc -> acc + node.pids_len end),
        nodes: inv_graph
      } 
    end
  end
end
