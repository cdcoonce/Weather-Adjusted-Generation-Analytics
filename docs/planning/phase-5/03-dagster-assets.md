# Phase 5.3 — Dagster Asset Integration Tests

## Objective
Validate that key Dagster assets can be materialized **in-process** (no UI) against an isolated DuckDB.

## Current Dagster entrypoint
- Definitions live in `dags/dagster_project/__init__.py` as `defs`.
- Assets live in `dags/dagster_project/assets/__init__.py`.

## What to test (recommended)
1. **Correlation asset** (`weather_generation_correlation`)
   - Seed a temp DuckDB with tiny `weather` and `generation` tables under the expected schema.
   - Materialize the asset and assert:
     - result contains `correlations` list
     - `total_records` > 0

2. **Combined ingestion asset** (`combined_ingestion`)
   - This asset calls `run_weather_ingestion()` and `run_generation_ingestion()`.
   - For *integration* tests, choose one:
     - Option A (practical): mock the ingestion calls and only validate that Dagster wiring works (asset executes, logs, completes).
     - Option B (more realistic): patch `src.config.config` paths to point at temp parquet dirs and allow ingestion to run against temp DuckDB.

## Harness options
- Use Dagster test helpers:
  - `materialize([...], resources={...})` for in-process execution.
  - Or `defs.get_implicit_global_asset_job_def()` / `define_asset_job` and run with Dagster instance.

## Isolation & patching guidance
- Many Dagster modules mutate `sys.path` to import `src/`. Keep tests explicit about imports.
- Assets import `src.config.config` (global). For deterministic tests:
  - patch module-level references (e.g., `dags.dagster_project.assets.config`) to a temp `Config` or a lightweight fake.

## Markers
- `@pytest.mark.integration`
- `@pytest.mark.dagster`
- Typically also `@pytest.mark.duckdb`
