defmodule Monet.MixProject do
	use Mix.Project

	@version "0.1.2"

	def project do
		[
			app: :monet,
			deps: deps(),
			elixir: "~> 1.10",
			version: @version,
			elixirc_paths: paths(Mix.env),
			description: "MonetDB driver",
			package: [
				licenses: ["MIT"],
				links: %{
					"git" => "https://github.com/karlseguin/monet"
				},
				maintainers: ["Karl Seguin"],
			],
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
			{:ex_doc, "~> 0.22.6", only: :dev, runtime: false},
			{:earmark, "~> 1.4.10", only: :dev, runtime: false},
			{:makeup_elixir, "~> 0.15.0", only: :dev, runtime: false}
		]
	end
end
