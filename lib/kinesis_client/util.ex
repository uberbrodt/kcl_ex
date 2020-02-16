defmodule KinesisClient.Util do
  @moduledoc false
  def optional_kw(keywords, _name, nil) do
    keywords
  end

  def optional_kw(keywords, name, value) do
    Keyword.put(keywords, name, value)
  end
end
