# Phase 1.4 — Smoke Test (Discovery + Imports)

## Objective
Add one tiny test that proves:
- pytest is installed and configured
- tests are discovered from `tests/`
- `src` imports resolve in the uv environment

## Proposed smoke test
Create:
- `tests/unit/test_smoke.py`

Suggested contents (conceptual):
- Import `src` (or a small module like `src.config.settings`).
- Assert a trivial invariant (e.g., `config` has expected default paths or types).

## What to avoid
- Don’t create files under `data/`.
- Don’t run ingestion, dbt, Dagster, or DuckDB queries.
- Keep runtime < 1 second.

## Commands to validate

```bash
uv run pytest
```

(Optional) run only smoke/unit tests:

```bash
uv run pytest -m unit
```

## Acceptance criteria
- `uv run pytest` reports 1 passing test.
- No network calls occur.
- No writes happen outside `tmp_path` (if you used it).
