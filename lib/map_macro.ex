defmodule MapMacro do
  defmacro dict([start|rest], init) do quote do %{unquote(start) => MapMacro.dict(unquote(rest), unquote(init))} end end
  defmacro dict([], init) do init end

  defmacro update(value, [start|rest], init, fun) do
    result = Macro.unique_var(:result, __MODULE__)
    quote do
      Map.update(unquote(value), unquote(start), MapMacro.dict(unquote(rest), unquote(init)), fn unquote(result) -> 
        MapMacro.update(unquote(result), unquote(rest), unquote(init), unquote(fun)) 
      end)
    end
  end

  defmacro update(value, [], _, fun) do
    quote do (unquote(fun)).(unquote(value)) end
  end

  defmacro update!(value, [start|rest], fun) do
    result = Macro.unique_var(:result, __MODULE__)
    quote do
      Map.update!(unquote(value), unquote(start), fn unquote(result) -> 
        MapMacro.update!(unquote(result), unquote(rest), unquote(fun)) 
      end)
    end
  end

  defmacro update!(value, [], fun) do
    quote do (unquote(fun)).(unquote(value)) end
  end

  defmacro get(value, [start|rest]) do
    quote do
      MapMacro.get(unquote(value) |> Map.get(unquote(start)), unquote(rest)) 
    end
  end

  defmacro get(value, []) do
    value
  end

  def map(input, fun) do 
    map(input, -1, fun)
  end

  def map(input, 0, fun) do 
    fun.(input)
  end

  def map(input, lvl, fun) do 
    if is_map(input) do
      Map.new(input, fn {key, value} -> {key, MapMacro.map(value, lvl-1, fun)} end)
    else
      fun.(input)
    end
  end

  def get_and_update(input, fun) do 
    if is_map(input) do
      new_map = input
      |> Map.new(fn {key, value} -> {key, MapMacro.get_and_update(value, fun)} end) 
      |> Map.filter(fn 
        {_k, :pop} -> false
        _ -> true
      end)

      if new_map==%{}, do: :pop, else: new_map
    else
      fun.(input)
    end
  end

  def merge(m1, m2) do
    Map.merge(m1, m2, fn 
      _k, v1, v2 when is_map(v1) or is_map(v2) -> merge(v1, v2)
      _k, v1, v2 -> Enum.random([v1, v2])
    end)
  end

  def count(m) do
    m 
      |> Map.values() 
      |> Enum.reduce(0, fn x, acc -> acc + (if is_map(x), do: count(x), else: 1) end) 
  end
end