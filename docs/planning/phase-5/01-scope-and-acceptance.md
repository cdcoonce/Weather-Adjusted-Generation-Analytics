# Phase 5.1 — Scope & Acceptance

## Scope
Phase 5 introduces integration tests that exercise real boundaries:

- **DuckDB**: read/write to a temp DuckDB file.
- **dlt**: (optional) run ingestion on *tiny* parquet inputs.
- **dbt**: run a *subset* of models against a temp DuckDB.
- **Dagster**: materialize selected assets in-process using Dagster test utilities.

## Out of scope
- Full historical mock data generation (2 years × hourly) — too heavy for tests.
- Running Dagster UI (`dagster dev`) in tests.
- Long-running dbt runs for all models.

## Proposed test taxonomy
- `@pytest.mark.integration`: required for all Phase 5 tests.
- Add the narrower markers as applicable:
  - `@pytest.mark.duckdb` for DuckDB file usage.
  - `@pytest.mark.dbt` for dbt CLI/API usage.
  - `@pytest.mark.dagster` for Dagster harness/materialization.
  - `@pytest.mark.io` when using filesystem.

## Acceptance criteria
- Integration suite runtime target: ~30–90 seconds on a typical laptop.
- Tests do not mutate repo state:
  - No writes under `data/`.
  - No writes under `dbt/renewable_dbt/target/` (except if dbt requires; prefer `--target-path` into `tmp_path`).
- All integration tests are deterministic:
  - Fixed tiny parquet inputs.
  - Fixed schema names.
  - No “current time” assertions.

## Suggested file layout
- `tests/integration/test_dbt_smoke.py`
- `tests/integration/test_dagster_assets_smoke.py`
- `tests/integration/test_e2e_smoke.py`
