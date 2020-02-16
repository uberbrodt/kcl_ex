defmodule KinesisClient.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kinesis_client,
      version: "0.2.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      package: package(),
      description: description(),
      deps: deps(),
      source_url: "https://github.com/uberbrodt/kcl_ex"
    ]
  end

  def description do
    """
    A library for creating Elixir clients for AWS Kinesis. Uses the Multilang
    Daemon recommended by Amazon.
    """
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
      links: %{Github: "https://github.com/uberbrodt/kcl_ex"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.0", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0.0-rc.7", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false}
    ]
  end
end
