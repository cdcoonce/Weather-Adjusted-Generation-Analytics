# Phase 5.4 — End-to-End Smoke Test

## Objective
Add one “happy path” test that proves the repo’s main workflow works on tiny inputs:

1. small parquet inputs
2. ingestion into DuckDB
3. dbt build (subset)
4. query a mart output

## Proposed approach
- Use the committed tiny parquet files under `tests/data/`.
- Use a temp DuckDB file.
- Use a temp dbt target directory (`--target-path`) to avoid mutating repo dbt artifacts.

## Steps
1. **Arrange**
   - Create `tmp_path / data/raw/weather` and `tmp_path / data/raw/generation`.
   - Copy sample parquet into those directories.
   - Patch runtime config so ingestion reads from temp raw dirs and writes to temp DuckDB.

2. **Act**
   - Run ingestion (either `run_full_ingestion` or `run_combined_pipeline`) against the temp DuckDB.
   - Run `dbt build --select <small selection>` against the same temp DuckDB.

3. **Assert**
   - Query DuckDB for an expected mart relation and assert row count > 0.

## Acceptance criteria
- The test is deterministic and doesn’t write to repo state.
- Runtime is bounded (target < 60–90 seconds).

## Marker usage
- `@pytest.mark.integration`
- `@pytest.mark.duckdb`
- `@pytest.mark.dbt`
- `@pytest.mark.io`
