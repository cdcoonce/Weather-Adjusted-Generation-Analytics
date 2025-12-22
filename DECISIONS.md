# DECISIONS.md

This file is the repository decision log.

## Rules

- Add an entry whenever we make a decision that is hard to reverse.
- Prefer bullets over paragraphs.
- Record: **Context → Decision → Consequences**.
- If a decision is not final, mark it as **Proposed** and add an owner + due date.

## Decision Index

- D-0001: Tooling baseline (uv, pytest, dbt, Dagster, dlt, DuckDB, Polars)
- D-0002: Test strategy (markers + warnings-as-errors)
- D-0003: Integration test strategy (temp DuckDB + temp dbt profiles)
- D-0004: Python version policy (Python 3.12+)
- D-0005: Canonical Python package + import strategy

---

## D-0001 — Tooling baseline (uv, pytest, dbt, Dagster, dlt, DuckDB, Polars)

- **Status:** Accepted
- **Date:** 2025-12-21

### D-0001 Context

- Repo is an analytics engineering pipeline using Dagster + dbt + dlt + DuckDB + Polars.
- Developer workflows and docs use `uv run ...`.

### D-0001 Decision

- Use `uv` for dependency management and running commands.
- Keep the core stack:
  - Orchestration: Dagster
  - Ingestion: dlt
  - Warehouse: DuckDB
  - Transformations: dbt
  - DataFrames: Polars-first
  - Testing: pytest

### D-0001 Consequences

- Local and CI docs MUST prefer `uv run ...` examples where feasible.
- Avoid introducing new frameworks/tools without explicit instruction.

---

## D-0002 — Test strategy (markers + warnings-as-errors)

- **Status:** Accepted
- **Date:** 2025-12-21

### D-0002 Context

- We want deterministic tests that catch breaking changes early.
- The project includes multiple layers (pure transforms, IO, dbt, Dagster).

### D-0002 Decision

- Use pytest markers to separate suites (`unit`, `integration`, and related sub-markers).
- Treat warnings (especially DeprecationWarnings) as errors to prevent silent drift.

### D-0002 Consequences

- Runtime deprecations in core code paths can fail tests; fix deprecations promptly.
- Integration tests MUST remain lightweight and deterministic.

---

## D-0003 — Integration test strategy (temp DuckDB + temp dbt profiles)

- **Status:** Accepted
- **Date:** 2025-12-21

### D-0003 Context

- We need confidence that dbt + DuckDB + Dagster work together.
- Tests MUST not write to repo state (`data/`, dbt `target/`) or require secrets.

### D-0003 Decision

- Integration tests MUST:
  - Use a temporary DuckDB file (or temp path) per test run.
  - Use temporary dbt `profiles.yml` and `--target-path` in temp dirs.
  - Assert dbt relations via `information_schema` (avoid hardcoding schema names).

### D-0003 Consequences

- Integration tests are portable across machines and CI.
- dbt model selection in tests MUST include dependencies (use `+model` selector when needed).

---

## D-0004 — Python version policy

- **Status:** Accepted
- **Date:** 2025-12-21

### D-0004 Context

- `README.md` states Python 3.11+.
- `AGENTS.md` states all code must be Python 3.12+ compatible.

### D-0004 Decision

- Standardize on **Python 3.12+** for local dev and CI.

### D-0004 Consequences

- `pyproject.toml` MUST specify `requires-python = ">=3.12"`.
- Docs MUST say Python 3.12+.
- CI MUST run Python 3.12+.

### D-0004 Validation

- Verified locally (macOS) on 2025-12-21 (Python 3.12.9):
  - `uv run pytest -m unit`
  - `uv run pytest -m integration`
  - `uv run pytest`

---

## D-0005 — Canonical Python package + import strategy

- **Status:** Accepted
- **Date:** 2025-12-21

### D-0005 Context

- The repo previously relied on importing from a top-level `src` package and/or mutating `sys.path` in Dagster modules.
- This was brittle across execution contexts (Dagster working directory, PYTHONPATH differences) and obscured the canonical API surface.

### D-0005 Decision

- The canonical import root is the real package: `weather_adjusted_generation_analytics`.
- Dagster modules MUST NOT mutate `sys.path` to make imports work.
- The legacy `src/` package is removed (no compatibility alias) to prevent drift.

### D-0005 Consequences

- All internal imports MUST use `weather_adjusted_generation_analytics.*`.
- Tooling (pytest coverage, CI lint targets, docs) MUST reference `weather_adjusted_generation_analytics/`.
- Any new modules MUST be added under `weather_adjusted_generation_analytics/`.
