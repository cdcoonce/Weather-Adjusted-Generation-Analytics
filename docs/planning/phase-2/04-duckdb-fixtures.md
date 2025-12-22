# Phase 2.4 — DuckDB Fixtures

## Objective
Provide DuckDB fixtures that make it trivial to:
- create a connection
- create schema + tables
- insert Polars DataFrames
- run simple SQL assertions

## Where code should live
- `tests/fixtures/duckdb_fixtures.py` for helpers
- `tests/conftest.py` for exporting fixtures to all tests

## Proposed fixtures

### `duckdb_conn_in_memory`
- Returns a `duckdb.DuckDBPyConnection` opened with `duckdb.connect(":memory:")`.
- Uses `yield` so the connection always closes.
- Mark tests that use this with `@pytest.mark.duckdb`.

### `duckdb_conn_file(temp_config)`
- Connects to `str(temp_config.duckdb_path)`.
- Useful when code expects a file path.

### `duckdb_load_weather_and_generation(duckdb_conn_in_memory, weather_df, generation_df)`
Helper fixture/function that:
- creates schema `renewable_energy` (or uses `temp_config.dlt_schema`)
- creates tables `weather` and `generation`
- inserts the data

Implementation note: DuckDB can ingest Arrow or Polars via `conn.register()` then `CREATE TABLE AS SELECT ...`.

## Test-level patterns enabled
- `SELECT COUNT(*)` assertions
- join sanity checks
- correlation input query validation (the SQL in Dagster asset)

## Acceptance criteria
- A test can run a join query between weather and generation.
- No connection leaks (connections close even on failure).
