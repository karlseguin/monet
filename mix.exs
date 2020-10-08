defmodule Monet.MixProject do
  use Mix.Project

  @source_url "https://github.com/karlseguin/monet"
  @version "0.1.2"

  def project do
    [
      app: :monet,
      name: "Monet",
      deps: deps(),
      elixir: "~> 1.10",
      version: @version,
      elixirc_paths: paths(Mix.env()),
      description: "MonetDB driver",
      package: package(),
      docs: docs()
    ]
  end

  defp paths(:test), do: paths(:prod) ++ ["test/support"]
  defp paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  defp deps do
    [
      {:jason, "~> 1.2.2"},
      {:decimal, "~> 2.0.0"},
      {:nimble_pool, "~> 0.2.1"},
      {:nimble_parsec, "~> 1.1.0"},
      {:ex_doc, "~> 0.22.6", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Karl Seguin"],
      licenses: ["ISC"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: [
        "README.md",
        "LICENSE"
      ]
    ]
  end
end
