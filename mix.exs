defmodule Monet.MixProject do
	use Mix.Project

	@version "0.1.6"

	def project do
		[
			app: :monet,
			deps: deps(),
			elixir: "~> 1.10",
			version: @version,
			elixirc_paths: paths(Mix.env),
			description: "MonetDB driver",
			test_coverage: [tool: ExCoveralls],
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
			{:nimble_pool, "~> 0.2.3"},
			{:nimble_parsec, "~> 1.1.0"},
			{:excoveralls, "~> 0.13.3", only: :test},
			{:ex_doc, "~> 0.23.0", only: :dev, runtime: false}
		]
	end
end
