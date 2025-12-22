# Phase 1.3 — Tests Layout & Conventions

## Objective
Create a predictable `tests/` layout that matches the repo’s shape and supports unit + integration tests.

## Proposed layout
Create the following directories:

```
tests/
  conftest.py
  unit/
  integration/
  fixtures/
  data/
```

### Rationale
- `unit/`: pure logic tests that run fast and don’t hit external systems.
- `integration/`: heavier tests that touch DuckDB, Dagster, or dbt artifacts.
- `fixtures/`: reusable fixtures/factories beyond what fits nicely in `conftest.py`.
- `data/`: small, committed sample data files for deterministic tests.

## Conventions

### Test scope
- Unit tests should not call:
  - `dlt.pipeline(...).run(...)`
  - Dagster execution
  - dbt CLI
- Integration tests may call those, but should be marked and easy to skip.

### Markers
- Mark unit tests explicitly (`@pytest.mark.unit`) once markers are registered.
- Mark integration tests explicitly (`@pytest.mark.integration`) and add a second marker for the system used (e.g., `duckdb`, `dagster`, `dbt`).

### Imports
- Import code via `src.*` (not relative filesystem hacks) wherever possible.
- Avoid importing Dagster modules in unit tests unless specifically testing Dagster definitions.

## Minimal `conftest.py` (Phase 1)
In Phase 1, keep `conftest.py` simple:
- A fixture that returns a `Config()` instance pointing at `tmp_path` (avoid writing to `data/`).
- A fixture for a temp DuckDB path (but don’t build tables yet; that’s Phase 2).

## Acceptance criteria
- `pytest` discovers tests inside `tests/`.
- The repo can support both unit and integration tests without rework.
