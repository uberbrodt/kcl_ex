defmodule KCL.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kcl,
      version: "0.1.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      package: package(),
      deps: deps(),
      source_url: "https://github.com/uberbrodt/kcl_ex"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      licenses: ["Apache 2.0"],
      maintainers: ["Chris Brodt"],
      links: %{"Github": "https://github.com/uberbrodt/kcl_ex"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:exjsx, "~> 4.0.0"},
      {:credo, "~> 0.8", only: [:dev, :test], runtime: false},
      {:timex, "~> 3.1.0"},
    ]
  end
end
