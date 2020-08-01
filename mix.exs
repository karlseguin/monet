defmodule Monet.MixProject do
	use Mix.Project

	@version "0.0.1"

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
			extra_applications: [:logger]
		]
	end

	defp deps do
		[
			{:jason, "~> 1.2.1"},
			{:decimal, "~> 1.8.1"},
			{:nimble_parsec, "~> 0.6.0"},
			{:ex_doc, "~> 0.21.2", only: :dev, runtime: false},
			{:nimble_pool, git: "https://github.com/dashbitco/nimble_pool.git", tag: "38800ef31ef07872f3d32fe163bd37ff93046d70"}
		]
	end
end
