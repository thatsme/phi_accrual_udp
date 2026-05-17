defmodule PhiAccrualUdp.MixProject do
  use Mix.Project

  @version "1.0.0"
  @source_url "https://github.com/thatsme/phi_accrual_udp"

  def project do
    [
      app: :phi_accrual_udp,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "phi_accrual_udp",
      source_url: @source_url,
      docs: docs(),
      dialyzer: dialyzer(),
      elixirc_options: [warnings_as_errors: true]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:phi_accrual, "~> 1.0"},
      {:telemetry, "~> 1.2"},
      {:stream_data, "~> 1.1", only: [:test, :dev]},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp dialyzer do
    [
      flags: [
        :underspecs,
        :unmatched_returns,
        :error_handling,
        :unknown,
        :extra_return,
        :missing_return
      ],
      plt_add_apps: [:phi_accrual, :telemetry, :stream_data],
      ignore_warnings: ".dialyzer_ignore.exs",
      list_unused_filters: true
    ]
  end

  defp description do
    "Dedicated UDP socket source for phi_accrual. " <>
      "Escapes BEAM distribution head-of-line blocking. " <>
      "Receiver-driven clock discipline; packet timestamp is diagnostic-only."
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md",
        "phi_accrual" => "https://hex.pm/packages/phi_accrual"
      },
      files: ~w(lib mix.exs README.md UPGRADING.md CHANGELOG.md LICENSE .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md", "LICENSE"],
      source_ref: "v#{@version}",
      source_url: @source_url,
      formatters: ["html"]
    ]
  end
end
