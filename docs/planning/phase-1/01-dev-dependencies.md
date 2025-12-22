# Phase 1.1 — Dev Dependencies

## Objective
Ensure the repo has all test tooling dependencies installed via `uv`, with versions tracked in `pyproject.toml` and `uv.lock`.

## Current state (baseline)
- `pytest` and `pytest-mock` are already present under `[project.optional-dependencies].dev`.
- `pytest-cov` is not currently listed.

## Steps

### 1) Add coverage tooling
Update `pyproject.toml` under `[project.optional-dependencies].dev`:
- Add `pytest-cov`

Suggested entry:
- `pytest-cov>=4.1.0`

Notes: version ranges are intentionally loose; keep aligned with your Python 3.11 baseline.

### 2) Sync environment with uv
After editing `pyproject.toml`, update the lock + environment:

```bash
uv sync --extra dev
```

If you prefer syncing all extras (if you later add more groups), use:

```bash
uv sync --all-extras
```

### 3) Quick validation
Confirm pytest is runnable through uv:

```bash
uv run pytest --version
```

## Acceptance criteria
- `pytest-cov` is installed in the dev environment.
- `uv.lock` changes reflect the new dependency.
- `uv run pytest --version` succeeds.
