# Phase 2.1 — Fixture Strategy

## Objective
Define a fixture approach that is:
- deterministic
- isolated (no shared state across tests)
- fast
- aligned with the repo’s architecture (config → parquet → dlt → duckdb → dbt → dagster)

## Guiding principles
- **No real data directory writes**: tests must not write into `data/`.
- **Prefer explicit config objects**: avoid relying on the global `config` instance from `src.config.settings`.
- **Use `tmp_path` everywhere**: any file IO should be scoped per-test.
- **Keep fixtures small**: if a fixture is > ~30 lines or has nontrivial logic, move it into `tests/fixtures/`.

## Proposed fixture tiers

### Tier A — Always-on, cheap
Defined in `tests/conftest.py`.
- `temp_config`: already exists; should remain the default config fixture.
- `temp_data_root`: returns the base temp path used for IO.

### Tier B — Data factories (pure)
Defined in `tests/fixtures/polars_factories.py`.
- `weather_df_small(...)` and `generation_df_small(...)` functions that return `polars.DataFrame` objects.
- Deterministic date range + deterministic values.

### Tier C — File builders (writes into temp)
Defined in `tests/fixtures/parquet_builders.py`.
- `write_weather_parquet(df, path)`
- `write_generation_parquet(df, path)`

### Tier D — DuckDB helpers
Defined in `tests/fixtures/duckdb_fixtures.py`.
- `duckdb_conn_in_memory`: returns a `duckdb.DuckDBPyConnection` that closes after the test.
- `duckdb_conn_file(temp_config)`: returns a connection to a temporary DB file.

## What is *not* in Phase 2
- Running `dlt.pipeline(...).run(...)` (reserve for integration tests later).
- Running dbt CLI (Phase 6/Phase integration).
- Dagster `Definitions` execution (later integration).

## Acceptance criteria
- A test can get a `polars.DataFrame`, write it to temp parquet, and read it back.
- A test can load both tables into DuckDB and join them.
- Fixtures have clear names and do not conflict.
