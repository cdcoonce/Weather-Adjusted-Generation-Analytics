# Phase 1 — Test Infra & Conventions (Roadmap)

These documents are a step-by-step playbook for implementing **Phase 1** of the pytest rollout.

## Goal
Stand up the test suite skeleton and standard conventions so that Phase 2+ can focus on writing tests rather than arguing about structure.

## What Phase 1 should produce
- A `tests/` directory with an agreed layout (`unit/`, `integration/`, `data/`, `fixtures/`).
- A baseline `tests/conftest.py` (even if it only contains a small number of fixtures).
- dev dependencies updated (notably `pytest-cov`).
- pytest configuration tightened (markers registered, warnings policy, consistent defaults).
- One tiny smoke test to validate imports and test discovery.

## Documents (recommended order)
1. `01-dev-dependencies.md`
2. `02-pytest-config.md`
3. `03-tests-layout.md`
4. `04-smoke-test.md`
5. `05-local-commands.md`

## Definition of done
- `uv run pytest` discovers and runs at least one test.
- `--strict-markers` does not fail due to missing marker registration.
- Test layout is in place and ready for Phase 2 fixtures.
