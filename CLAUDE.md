# Weather Adjusted Generation Analytics (WAGA)

This file is auto-loaded every conversation. It defines how Claude should work in this repo.

## What This Repo Is

A data engineering pipeline for analyzing renewable energy asset performance with weather correlation analysis. Built with Dagster (orchestration), dbt (transformation), dlt (ingestion), Polars (analytics), and Snowflake (warehouse).

## Architecture

```text
Parquet/API sources
  -> dlt ingestion assets (Snowflake destination, merge disposition)
  -> Snowflake WAGA database (RAW schema)
  -> dbt transformations (@dbt_assets in Dagster)
     staging (views) -> intermediate (ephemeral) -> marts (tables, contracted)
  -> Polars analytics assets (LazyFrame API, ANALYTICS schema)
  -> Dashboard layer (MARTS + semantic layer)
```

### Package Layout

- `src/weather_analytics/` — **Active** Dagster package (Snowflake-based)
  - `definitions.py` — Dagster Cloud entry point
  - `assets/ingestion/` — dlt ingestion (weather, generation)
  - `assets/analytics/` — Polars correlation analysis
  - `assets/dbt_assets.py` — `@dbt_assets` wrapper (requires manifest.json)
  - `resources/` — `WAGASnowflakeResource`, `DltIngestionResource`
  - `checks/` — freshness, row count, value range asset checks
  - `schedules.py` — daily ingestion, daily dbt, weekly analytics
  - `lib/` — `polars_utils.py` (LazyFrame), `config.py` (Pydantic Settings), `logging.py`
  - `cockpit/` — self-contained static dashboard (Jinja + inline SVG, no chart lib); `build`/`serve`/`deploy` CLI
  - `mock_data/` — data generators **and** the local fleet simulation:
    - `fleet.py` — 12-asset mixed fleet (wind/solar/battery/gas) at real US lat/lon
    - `physics.py` — vectorized plant physics (power curves, SOC dispatch, heat rate)
    - `weather_sources.py` — Open-Meteo real weather + synthetic fallback
    - `simulate.py` — weather + physics → hourly generation (merit-order dispatch)
    - `local_export.py` — build the 4 dashboard JSON files with **no Snowflake**
    - `generate_generation.py` / `generate_weather.py` — legacy wind/solar generators feeding the Snowflake ingestion path
- `dbt/renewable_dbt/` — dbt project (dbt-snowflake)
  - `models/staging/{weather,generation}/` — per-source subfolders
  - `models/intermediate/` — ephemeral models
  - `models/marts/` — contracted tables
  - `models/semantic_models/` — dbt metrics layer
  - `profiles/profiles.yml` — Snowflake key-pair auth with env var templating
- `tests/` — pytest suite (unit tests only)

### Snowflake Schemas

| Schema      | Purpose                              |
| ----------- | ------------------------------------ |
| `RAW`       | dlt landing zone (merge disposition) |
| `STAGING`   | dbt views                            |
| `MARTS`     | dbt contracted tables                |
| `ANALYTICS` | Polars outputs                       |

## Commands

- Run tests: `uv run pytest -m unit`
- Run with coverage: `uv run pytest -m unit --cov=src/weather_analytics --cov-report=term-missing`
- Lint: `uv run ruff check src/weather_analytics/`
- Format: `uv run ruff format src/weather_analytics/`
- Type check: `uv run mypy src/weather_analytics/`
- Rebuild dashboard (no Snowflake): `uv run python scripts/build_local_dashboard.py --start 2025-07-01 --end 2026-06-30 --build` (add `--synthetic` for a deterministic offline run)
- Preview / deploy dashboard: `uv run python -m weather_analytics.cockpit serve` / `... deploy`
- Regenerate the asset-dimension seed after editing `fleet.FLEET`: `uv run python scripts/generate_asset_seed.py` (writes `dbt/renewable_dbt/seeds/asset_dimension.csv`), then `dbt seed` to load it

## Code Style

- Python >= 3.12, strict typing (`mypy --strict`-adjacent)
- `X | None` union syntax (PEP 604), not `Optional[X]`
- Type hints on ALL function signatures (params + return)
- Numpy-style docstrings for public functions
- Descriptive variable names (`private_key_bytes` not `pkb`)
- SOLID, DRY, YAGNI — simplicity over complexity
- Line length: 88 (ruff)
- Ruff rules: see `pyproject.toml` `[tool.ruff]` section
- Tests: pytest markers (`unit`, `integration`, `io`, `dagster`, `dbt`, `snowflake`)
- **Do NOT use `from __future__ import annotations`** in files with Dagster decorators (`@asset`, `@asset_check`, `@dbt_assets`) — Dagster needs concrete type annotations

## Conventions

- All Dagster assets/schedules/sensors prefixed `waga_`
- All Snowflake env vars prefixed `WAGA_` (see `.env.example`)
- Snowflake auth: key-pair with service account (no passwords)
- dbt models: staging = views, intermediate = ephemeral, marts = tables with contracts
- dbt staging organized into per-source subfolders (`weather/`, `generation/`)
- New data source = new ingestion asset + staging subfolder. Pure additive.
- dbt manifest generated at build time (`dbt parse`); code handles missing manifest gracefully
- **One fleet, two build paths.** The 12-asset `mock_data/fleet.FLEET` (4 wind, 4 solar, 2 battery, 2 gas) is the single source of truth. It feeds **both** the Snowflake pipeline (ingestion → RAW → dbt marts → `dashboard_export.py`) and the no-warehouse local path (`simulate.py` → `local_export.py`). Both emit the identical **v2.0** export schema.
- **Explicit `asset_type`, not inferred.** RAW/staging/marts carry an explicit `asset_type` column (`accepted_values: ['wind','solar','battery','gas']`); the weather mart's `inferred_asset_type` is now that explicit value, not a wind-vs-solar correlation guess.
- **Asset dimension.** Site name / coordinates / region live in the `asset_dimension` dbt seed (generated from `fleet.FLEET` by `scripts/generate_asset_seed.py`) → `dim_asset` mart (`WAGA.MARTS.dim_asset`, contracted). `dashboard_export` joins it so assets.json carries real names + lat/lon like the local path. **Don't hand-edit the seed CSV** — regenerate it from `fleet.FLEET`.
- **Type-branched scoring** (`mart_asset_weather_performance`): weather-adjusted ratio for wind/solar, realized round-trip efficiency (discharge/charge) for battery, heat-rate efficiency (best/realized) for gas.
- **Battery is a net load.** A charging battery has negative `net_generation_mwh` and capacity factor. Non-negativity range tests (RAW source, staging, daily mart) and the `waga_generation_value_range_check` are all scoped `where asset_type <> 'battery'`.
- **Ingestion is deterministic per partition** (`_partition_seed(partition_key)`), so re-runs merge idempotently and the weather/generation assets stay mutually consistent. Ingestion weather is synthetic; the live Open-Meteo pull is a local-dashboard feature.
- `cockpit/data.py` tolerates missing/extra fields, and the manifest carries `weather_source` (`open-meteo` / `synthetic` / `snowflake`) driving the dashboard's data-source badge.
- Ingestion assets use `op_tags={"dagster/concurrency_key": "waga_ingestion"}` to prevent concurrent merges

## Key Design Decisions

- **Centralized auth**: `WAGASnowflakeResource.get_connection()` is the single Snowflake auth path. dlt and Polars assets use this.
- **LazyFrame-first**: All Polars utility functions accept/return `pl.LazyFrame`. Caller decides when to `.collect()`.
- **Empty mart guard**: Analytics assets raise `dagster.Failure` if source mart has fewer than 10 rows.
- **Merge idempotency**: dlt uses `write_disposition="merge"` on `(asset_id, timestamp)` — running ingestion twice produces no duplicates.

## CI / Deployment

- **`ci.yml`** — Lint (ruff) + unit tests on every push/PR to main
- **`deploy.yml`** — Dagster Cloud serverless prod deploy on push to main (test → dbt manifest → deploy)
- **`branch_deployments.yml`** — Ephemeral branch deploys on PRs for preview environments
- **Dagster Cloud org**: `charles-likes-data.dagster.plus`
- **dbt manifest**: Generated in deploy workflows via `dbt parse` with Snowflake secrets injected
- **`_ensure_key_file()`**: Decodes `WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64` to temp `.p8` file at runtime for dbt-snowflake

## Planning

Write plans to `docs/plans/`. Archive completed plans to `docs/archive/`.
Dev cycle state tracked in `docs/dev-cycle/`.
