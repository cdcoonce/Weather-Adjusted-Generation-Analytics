# ARCHITECTURE.md

This document is the architecture reference for this repository.

## Purpose

- Describe system boundaries and data flow.
- Provide a stable mental model for contributors and agents.
- Reduce re-discovery across sessions.

## System overview

This repository implements a small, production-style analytics engineering pipeline:

- **Mock data generation** (Polars) → **Parquet** (daily partitions)
- **Ingestion** (dlt) → **DuckDB warehouse**
- **Transformations** (dbt) → **Staging / Intermediate / Marts**
- **Orchestration** (Dagster) to schedule and wire the steps

## Boundaries (stable)

### Dagster

- Dagster is **orchestration only**.
- Dagster assets SHOULD be thin wrappers that call testable logic.
- Dagster code lives under `dags/dagster_project/`.

### Business logic

- Business logic SHOULD be deterministic and testable.
- Core Python logic lives under `src/`.
- dbt SQL logic lives under `dbt/renewable_dbt/`.

### Storage

- DuckDB is the embedded warehouse.
- Parquet is the raw file format used for ingestion.

## Data flow (end-to-end)

1. **Generate mock data**
   - Scripts:
     - `src/mock_data/generate_weather.py`
     - `src/mock_data/generate_generation.py`
   - Output (default):
     - `data/raw/weather/weather_YYYY-MM-DD.parquet`
     - `data/raw/generation/generation_YYYY-MM-DD.parquet`

2. **Ingest into DuckDB via dlt**
   - Entrypoint:
     - `src/loaders/dlt_pipeline.py`
   - Key behaviors:
     - Incremental load semantics are managed by dlt.
     - Composite primary key is typically `(asset_id, timestamp)`.

3. **Transform via dbt**
   - Project root:
     - `dbt/renewable_dbt/`
   - Model layers:
     - `models/staging/` (`stg_*`)
     - `models/intermediate/` (`int_*`)
     - `models/marts/` (`mart_*`)

4. **Orchestrate with Dagster**
   - Dagster assets run ingestion + dbt + analysis (where defined).
   - Dagster schedules/sensors can trigger ingestion when new Parquet arrives.

## dbt schemas and querying conventions

- Schema names may be adapter-specific (DuckDB commonly prefixes schemas).
- For tests and automated checks, prefer `information_schema.tables` lookups by `table_name` rather than hardcoding `table_schema`.

## Testing architecture

### Markers

- `unit`: pure or mocked tests (fast)
- `integration`: cross-component tests (dbt/Dagster/DuckDB)

### Integration testing principles

- MUST use temp directories and temp DuckDB paths.
- MUST avoid writing to repo state (no changes under `data/` or dbt `target/`).
- Prefer schema-agnostic assertions via `information_schema`.

## Configuration & environment

- Configuration is environment-driven (see `.env.example`).
- Paths in configs default to repo-relative directories.

## Repo memory hierarchy

When resuming work, consult (in order):

1. `AGENTS.md`
2. `README.md`
3. `ARCHITECTURE.md`
4. `DECISIONS.md`
5. `TODO.md`

