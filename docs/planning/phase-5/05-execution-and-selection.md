# Phase 5.5 — Execution & Selection

## Commands

### Run all integration tests

```bash
uv run pytest -m integration
```

### Run the full test suite (unit + integration)

```bash
uv run pytest
```

### Run only dbt integration tests

```bash
uv run pytest -m "integration and dbt"
```

### Run only Dagster integration tests

```bash
uv run pytest -m "integration and dagster"
```

### Run only end-to-end smoke

```bash
uv run pytest -m "integration and dbt and duckdb" -k e2e
```

## Local prerequisites

- Ensure dependencies are installed: `uv sync --extra dev`
- Ensure dbt deps are available:
  - `cd dbt/renewable_dbt && uv run dbt deps`

## CI notes (Phase 6 will formalize)

- Run `uv sync --extra dev`.
- Run `uv run dbt deps` in `dbt/renewable_dbt`.
- Run `uv run pytest -m "unit or integration"` (or split jobs).

## Reference result

- Verified locally (macOS) on 2025-12-20: `uv run pytest` and `uv run pytest -m integration`.
- Re-verified locally (macOS) on 2025-12-21 (Python 3.12.9): `uv run pytest` and `uv run pytest -m integration`.
