# Phase 1.2 — Pytest Configuration

## Objective
Make pytest configuration explicit and stable so tests run the same locally and in CI.

## Current state (baseline)
Pytest is configured in `pyproject.toml` under `[tool.pytest.ini_options]` with:
- `testpaths = ["tests"]`
- `python_files = ["test_*.py"]`
- `addopts = "-v --strict-markers"`

This is a good start, but once we introduce markers and integration tests, `--strict-markers` requires marker registration.

## Recommended configuration changes

### 1) Register markers
Add a `markers` list under `[tool.pytest.ini_options]`.

Suggested markers (keep minimal and meaningful):
- `unit`: fast, isolated tests
- `integration`: hits external systems or heavier runtime
- `io`: reads/writes filesystem
- `duckdb`: uses DuckDB connections
- `dagster`: runs Dagster assets/jobs
- `dbt`: runs or validates dbt SQL / compiled models

### 2) Add a warnings policy
Optional but recommended to prevent noisy test output. Add:
- `filterwarnings = ["error::DeprecationWarning", "error::PendingDeprecationWarning"]`

If third-party libs are noisy, switch to a narrower policy later; start strict and relax only where needed.

### 3) Prepare coverage defaults (requires pytest-cov)
Once `pytest-cov` is installed (Phase 1.1), update `addopts` to include coverage:

- `--cov=src`
- `--cov-report=term-missing`

Example `addopts` (single string):
- `-v --strict-markers --cov=src --cov-report=term-missing`

Keep the coverage threshold for a later phase (CI phase). In Phase 1 we just want coverage output to work.

## Acceptance criteria
- Running `uv run pytest` does not fail due to unknown markers.
- `uv run pytest -m "unit"` works once unit tests exist.
- If `pytest-cov` is enabled, `uv run pytest` prints a coverage summary.
