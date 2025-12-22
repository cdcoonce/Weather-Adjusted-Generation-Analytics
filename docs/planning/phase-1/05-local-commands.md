# Phase 1.5 — Local Commands & Developer Workflow

## Objective
Standardize how we run tests locally (with `uv`) so Phase 2+ work stays consistent.

## Commands

### Run everything

```bash
uv run pytest
```

### Run unit tests only
Once markers exist:

```bash
uv run pytest -m unit
```

### Run integration tests only

```bash
uv run pytest -m integration
```

### Exclude integration tests (common default)

```bash
uv run pytest -m "not integration"
```

### Run a single test file

```bash
uv run pytest tests/unit/test_smoke.py
```

### Show available markers

```bash
uv run pytest --markers
```

## Practical defaults
- Default local run should usually exclude integration tests while iterating:
  - `uv run pytest -m "not integration"`
- Integration tests should be run intentionally (and later in CI).

## Acceptance criteria
- All commands above work once Phase 1 is implemented.
- Marker selection behaves predictably with `--strict-markers`.
