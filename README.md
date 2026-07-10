# Weather Adjusted Generation Analytics (WAGA)

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A production-grade analytics pipeline for weather-adjusted renewable energy asset performance analysis. Built with Dagster (orchestration), dlt (ingestion), dbt (transformation), Polars (analytics), and Snowflake (warehouse).

---

## Architecture

```
Parquet / API sources
  -> dlt ingestion assets (Snowflake destination, merge disposition)
  -> Snowflake WAGA database (RAW schema)
  -> dbt transformations (@dbt_assets in Dagster)
     staging (views) -> intermediate (ephemeral) -> marts (tables, contracted)
  -> Polars analytics assets (LazyFrame API, ANALYTICS schema)
  -> dbt semantic layer (metrics for BI tools)
```

**Data flow:**

1. **Ingestion** -- dlt reads Parquet files and merges into `WAGA.RAW` on `(asset_id, timestamp)`
2. **Staging** -- dbt views in `WAGA.STAGING` apply initial transformations
3. **Intermediate** -- Ephemeral dbt models for daily aggregations and asset-weather joins
4. **Marts** -- Contracted dbt tables in `WAGA.MARTS` with enforced column types
5. **Analytics** -- Polars correlation analysis writes to `WAGA.ANALYTICS`
6. **Semantic layer** -- dbt metrics (`daily_generation`, `generation_efficiency`) for downstream BI

All orchestrated by Dagster with asset checks (freshness, row count, value range). Unattended execution is **local**: macOS `launchd` agents on the machine that owns the `.env` run the daily/weekly jobs (Dagster Cloud was retired for cost) — see [docs/local-scheduling.md](docs/local-scheduling.md).

---

## Data Model

### Source Tables (RAW)

| Table        | Key Columns                                                                                                                       |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `weather`    | `asset_id`, `timestamp`, `wind_speed_mps`, `ghi`, `temperature_c`, `pressure_hpa`, `relative_humidity`                            |
| `generation` | `asset_id`, `timestamp`, `gross_generation_mwh`, `net_generation_mwh`, `curtailment_mwh`, `availability_pct`, `asset_capacity_mw` |

### Mart Tables

| Table                            | Purpose                                                                       |
| -------------------------------- | ----------------------------------------------------------------------------- |
| `mart_asset_performance_daily`   | Daily capacity factors, generation summaries, availability metrics            |
| `mart_asset_weather_performance` | Weather-normalized performance, correlation coefficients, performance scoring |

### Key Metrics

- **Capacity Factor** = Net Generation (MWh) / (Asset Capacity (MW) x Hours)
- **Rolling Correlations** -- 7-day and 30-day Pearson correlations (wind speed <-> generation, GHI <-> generation)
- **Performance Score** -- Normalized 0-100 comparing actual vs. expected generation

---

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- Snowflake account with key-pair authentication

### Installation

```bash
git clone https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics.git
cd Weather-Adjusted-Generation-Analytics
uv sync
cp .env.example .env
# Edit .env with your WAGA_SNOWFLAKE_* credentials
```

### Environment Variables

All prefixed `WAGA_` -- see `.env.example` for the full list:

| Variable                            | Description                     |
| ----------------------------------- | ------------------------------- |
| `WAGA_SNOWFLAKE_ACCOUNT`            | Snowflake account identifier    |
| `WAGA_SNOWFLAKE_USER`               | Service account username        |
| `WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64` | Base64-encoded PEM private key  |
| `WAGA_SNOWFLAKE_PRIVATE_KEY_PATH`   | Path to PEM file (for dbt)      |
| `WAGA_SNOWFLAKE_WAREHOUSE`          | Compute warehouse               |
| `WAGA_SNOWFLAKE_DATABASE`           | Database name (default: `WAGA`) |
| `WAGA_SNOWFLAKE_ROLE`               | Snowflake role                  |

### Run Dagster

```bash
uv run dagster dev
```

Access the Dagster UI at http://localhost:3000 to materialize assets.

### Scheduled (unattended) runs

Daily ingestion + dbt + dashboard build **and Cloudflare Pages deploy**, and
the weekly correlation analysis, run locally via macOS `launchd` agents —
there is no hosted scheduler. The agents invoke `scripts/run_scheduled.py
<job>` against yesterday's partition and log to `logs/`; the runner waits for
the network at wake, `uv sync`s the environment up front, and retries each
step once. Install, schedule times, health checks, and the missed-run
catch-up runbook are in [docs/local-scheduling.md](docs/local-scheduling.md).

```bash
# Health check: second column is the last exit code (0 = healthy)
launchctl list | grep waga

# The green signal: last line of the newest daily log
tail -1 "$(ls -t logs/scheduled-daily-*.log | head -1)"

# Run a job manually (targets yesterday's partition)
uv run python scripts/run_scheduled.py daily
```

### Run dbt

```bash
cd dbt/renewable_dbt
uv run dbt deps
uv run dbt build --profiles-dir profiles
```

### Run Tests

```bash
uv run pytest -m unit
```

---

## Local Dashboard & Fleet Simulation

The static [cockpit dashboard](https://waga-dashboard.pages.dev) can be rebuilt
end-to-end **without Snowflake** from a local, physics-based fleet simulation.
This drives the deployed page and matches the style of the
[charleslikesdata.com](https://charleslikesdata.com) portfolio.

### The fleet

`src/weather_analytics/mock_data/fleet.py` defines a 12-asset mixed-technology
fleet at real US sites: **onshore wind** (turbine power curve + air-density
correction + AR(1) turbulence), **utility solar PV** (NOCT cell-temperature
derate + inverter clipping + cloud transients), **grid battery storage**
(SOC-bounded arbitrage dispatch, round-trip losses), and **natural gas** (CCGT +
peaker, merit-order dispatch, part-load heat rate, forced outages, CO₂).

### Real-time weather

By default the simulation pulls genuine hourly ERA5 weather for each asset's
latitude/longitude from the free [Open-Meteo archive API](https://open-meteo.com)
(no API key). If the network is unavailable it falls back to a latitude-aware
synthetic model, so the pipeline always runs. The dashboard header shows which
source was used.

### Rebuild the dashboard

```bash
# Full year of real weather -> exports + rendered dist/index.html
uv run python scripts/build_local_dashboard.py \
    --start 2025-07-01 --end 2026-06-30 --build

# Deterministic offline run (synthetic weather)
uv run python scripts/build_local_dashboard.py \
    --start 2026-01-01 --end 2026-03-31 --synthetic --build

# Then serve or deploy the static page
uv run python -m weather_analytics.cockpit serve   # local preview
uv run python -m weather_analytics.cockpit deploy  # Cloudflare Pages
```

The four `dashboard_exports/*.json` files (schema v2.0) carry per-technology
metrics — battery SOC/throughput, gas fuel/heat-rate/CO₂ — alongside the shared
generation/capacity-factor columns.

> **One fleet, both paths.** The same 12-asset fleet flows through the Snowflake
> pipeline too: ingestion emits an explicit `asset_type` plus battery/gas
> columns, the dbt marts branch scoring by technology (weather-adjusted for
> wind/solar, realized round-trip efficiency for battery, heat-rate efficiency
> for gas), and the export produces the identical v2.0 schema. The local path is
> the no-warehouse way to build the same dashboard.

---

## Project Structure

```
Weather_Adjusted_Generation_Analytics/
├── src/weather_analytics/          # Dagster pipeline package
│   ├── definitions.py              # Dagster entry point (dagster dev + scheduled CLI runs)
│   ├── assets/
│   │   ├── ingestion/              # dlt ingestion (weather, generation)
│   │   ├── analytics/              # Polars correlation analysis
│   │   └── dbt_assets.py           # @dbt_assets wrapper
│   ├── resources/                  # WAGASnowflakeResource, DltIngestionResource
│   ├── checks/                     # Asset checks (freshness, row count, range)
│   ├── schedules.py                # Daily/weekly schedules
│   ├── lib/                        # polars_utils, config, logging
│   ├── cockpit/                    # Self-contained static dashboard (build/serve/deploy)
│   └── mock_data/                  # Fleet simulation + data generators
│       ├── fleet.py                # 12-asset mixed fleet registry (real US sites)
│       ├── physics.py              # Wind/solar/battery/gas physics models
│       ├── weather_sources.py      # Open-Meteo real weather + synthetic fallback
│       ├── simulate.py             # Weather + physics -> hourly generation
│       ├── local_export.py         # Build dashboard JSON without Snowflake
│       └── generate_*.py           # Legacy wind/solar generators (Snowflake path)
│
├── scripts/
│   ├── build_local_dashboard.py    # Simulate -> export -> render the dashboard
│   ├── run_scheduled.py            # Job runner the launchd agents invoke
│   └── install_launchd.py          # Write/load the launchd agents (see docs/local-scheduling.md)
│
├── dbt/renewable_dbt/              # dbt project (dbt-snowflake)
│   ├── models/
│   │   ├── staging/{weather,generation}/
│   │   ├── intermediate/
│   │   ├── marts/                  # incl. dim_asset (asset dimension)
│   │   └── semantic_models/
│   ├── seeds/                      # asset_dimension.csv (generated from fleet.FLEET)
│   └── profiles/                   # Snowflake key-pair auth
│
├── tests/                          # pytest unit tests
├── docs/local-scheduling.md        # launchd scheduling: install, health checks, catch-up runbook
├── .env.example                    # Environment variable template
└── pyproject.toml                  # Dependencies and tooling
```

---

## Development

### Code Quality

```bash
uv run ruff check src/weather_analytics/    # Lint
uv run ruff format src/weather_analytics/   # Format
uv run mypy src/weather_analytics/          # Type check
uv run pytest -m unit                       # Tests
```

### Conventions

- All Dagster assets prefixed `waga_`
- All env vars prefixed `WAGA_`
- Python 3.12+ with strict typing, PEP 604 unions
- Numpy-style docstrings on public functions
- dbt: staging = views, intermediate = ephemeral, marts = contracted tables

---

## License

MIT License -- see the LICENSE file for details.

---

**Built with Dagster, dbt, dlt, Polars, and Snowflake**
