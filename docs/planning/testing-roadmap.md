# Testing Roadmap & Conventions

This file complements the phased plan with concrete steps, conventions, and example commands.

## Quick start — install and run

```bash
# Install dev dependencies (example using pip)
python -m pip install -U pip
pip install -r requirements-dev.txt

# Run tests with verbosity and coverage
pytest -q --cov=src --cov-report=term-missing
```

## Test folder layout
- `tests/` — root for all tests.
  - `tests/unit/` — unit tests (fast, isolated).
  - `tests/integration/` — integration tests (dbt, dagster, I/O heavy).
  - `tests/data/` — small committed sample files for deterministic tests.
  - `tests/fixtures/` — reusable fixtures and helpers.

## Naming conventions
- Test files: `test_*.py`.
- Test functions: `test_<functionality>_<edge>`.
- Use descriptive names and small, focused tests.

## Fixtures to implement (suggested)
- `duckdb_in_memory`: returns a connection to an in-memory DuckDB instance populated with small sample tables.
- `polars_df_factory`: helper to create small `polars.DataFrame` objects for tests.
- `sample_file_dir`: a `tmp_path`-based directory populated with committed `tests/data/` samples.
- `env_vars`: fixture to temporarily set environment variables (`monkeypatch.setenv`).

## Mocking patterns
- For HTTP/API calls: use `requests-mock` or monkeypatch the client method to return a deterministic response object.
- For file I/O: use `tmp_path` and assert on filesystem state.
- For heavy external tools (dbt): run compiled SQL against DuckDB instead of running full dbt unless necessary.

## dbt & DuckDB integration testing
- Use compiled SQL from `dbt/target/compiled/renewable_dbt/` and execute queries with the `duckdb` Python package against an in-memory DB populated with `tests/data/` source tables.
- Keep integration tests that run dbt SQL focused and small to avoid long CI times.

## Dagster tests
- Use Dagster's `build_init_resource_context` and test harness to run assets or jobs in-process.
- Provide lightweight mock resources for external dependencies.

## CI recommendations
- GitHub Actions workflow to run `pytest` matrix on Python versions used in `pyproject.toml`.
- Cache pip/poetry dependencies and DuckDB wheel if needed.
- Fail the job when coverage falls below the chosen threshold.

## Measurement & maintenance
- Start with a modest coverage gate (60–70%), raise gradually.
- Add tests as part of the feature PRs (TDD encouraged).
- Keep test data small and deterministic; avoid real external calls in CI.

## Next steps (developer checklist)
- Add `requirements-dev.txt` or update `pyproject.toml` with dev-deps.
- Create `tests/conftest.py` with core fixtures listed above.
- Implement initial unit tests for `src/utils` and `src/config`.
- Create a lightweight GitHub Actions workflow to run tests and publish coverage.

