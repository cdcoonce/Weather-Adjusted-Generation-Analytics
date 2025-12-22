# Phase 2.5 — Markers & Test Selection

## Objective
Ensure the new fixtures map cleanly to markers so we can:
- keep the default suite fast
- run heavier subsets intentionally

## Marker mapping
- `unit`: tests that use only pure Python/Polars logic and `tmp_path`.
- `io`: any test that writes/reads files (Parquet, temp directories).
- `duckdb`: any test that creates a DuckDB connection.
- `integration`: reserved for tests that run dlt/dbt/Dagster (later phases).

## Default run strategy
Recommended local default while iterating:

```bash
uv run pytest -m "unit and not integration"
```

## Example “future-proof” patterns
- A DuckDB-backed unit test could be:
  - `@pytest.mark.unit`
  - `@pytest.mark.duckdb`

- A later dlt ingestion test should be:
  - `@pytest.mark.integration`
  - `@pytest.mark.io`
  - (optionally) `@pytest.mark.duckdb`

## Acceptance criteria
- Marker usage stays consistent across the suite.
- We can exclude heavier tests reliably in CI until we’re ready.
