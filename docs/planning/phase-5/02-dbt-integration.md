# Phase 5.2 — dbt Integration Tests

## Objective
Verify that dbt can build a minimal slice of the project against an **isolated DuckDB database**.

## Key constraint: profiles portability
Current dbt profiles use an absolute path (see `dbt/renewable_dbt/profiles/profiles.yml`). For integration tests and CI, prefer making the DuckDB path configurable via environment variables.

Recommended approach (plan):
- Update `profiles.yml` to use `{{ env_var('DUCKDB_PATH') }}` for `path`.
- In tests/CI, set `DUCKDB_PATH` to a temp DuckDB path.

Alternative approach (no repo change):
- In tests, create a temporary `profiles.yml` under `tmp_path` and call dbt with `--profiles-dir`.

## What to test
Minimal smoke assertions:
- `dbt deps` succeeds (only needed once locally; in CI run it in setup).
- `dbt build --select ...` succeeds for a limited selection.
- Expected relation(s) exist in DuckDB after build.

Suggested selection (keep it small):
- staging: `stg_weather`, `stg_generation`
- one intermediate: `int_asset_weather_join`
- one mart: `mart_asset_performance_daily` (or `mart_asset_weather_performance`)

## Suggested test mechanics
- Use a temp DuckDB file path: `tmp_path / 'warehouse.duckdb'`.
- Ensure ingestion tables exist first (choose one):
  - Option A (fastest): create `renewable_energy.weather` and `renewable_energy.generation` directly using DuckDB + Polars fixtures.
  - Option B (more realistic): run the dlt loaders on `tests/data/*.parquet` into the temp DuckDB.
- Run dbt via subprocess:
  - `uv run dbt build --project-dir dbt/renewable_dbt --profiles-dir <tmp_profiles_dir> --target dev --select ... --target-path <tmp_target_path>`

## Assertions
- DuckDB contains the expected schema(s) and at least one expected table/view.
- Optionally assert a row count > 0 for one mart table.

## Notes
- Keep subprocess output captured on failure for debugging.
- Avoid running dbt for *all* models in integration tests (Phase 6 CI can add a nightly job if desired).
