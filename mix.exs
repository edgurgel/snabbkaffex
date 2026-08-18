defmodule Snabbkaffex.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/edgurgel/snabbkaffex"

  def project do
    [
      app: :snabbkaffex,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      docs: docs(),
      deps: deps(),
      dialyzer: [
        plt_local_path: "priv/plts",
        plt_core_path: "priv/plts",
        plt_add_apps: [:mix]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp description do
    "A thin Elixir wrapper around the Erlang snabbkaffe trace-based testing library."
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      maintainers: ["Eduardo Gurgel Pinho"],
      links: %{
        "GitHub" => @source_url,
        "snabbkaffe" => "https://github.com/kafka4beam/snabbkaffe"
      },
      files: ~w(lib mix.exs README.md CHANGELOG.md LICENSE .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      name: "Snabbkaffex",
      source_url: @source_url,
      source_ref: "v#{@version}",
      extras: ["README.md", "CHANGELOG.md", "LICENSE"]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:snabbkaffe, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
