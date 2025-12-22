# Phase 2 — Shared Fixtures & Test Data (Roadmap)

Phase 2 is about creating **stable building blocks** for the rest of the test suite:
- pytest fixtures that avoid side effects
- small deterministic datasets (Polars + Parquet)
- lightweight DuckDB helpers

This phase should *not* introduce a lot of tests yet; it should make Phase 3+ fast to write and hard to make flaky.

## Goal
Provide reusable, deterministic fixtures so that unit tests and integration tests can be written with minimal boilerplate and without relying on real `data/` or a developer’s `.env`.

## What Phase 2 should produce
- `tests/fixtures/` modules for:
  - Polars DataFrame factories
  - Parquet sample data builders
  - DuckDB in-memory / temp-db helpers
- A small committed `tests/data/` seed dataset
  - A couple of tiny Parquet files for weather + generation
  - Optional: a tiny dbt-friendly seed CSV if we need it later
- `tests/conftest.py` expanded with fixtures that:
  - create temp paths
  - return a temp-rooted `Config`
  - provide a DuckDB connection scoped to the test

## Documents (recommended order)
1. `01-fixture-strategy.md`
2. `02-polars-factories.md`
3. `03-sample-parquet-data.md`
4. `04-duckdb-fixtures.md`
5. `05-markers-and-test-selection.md`

## Definition of done
- Fixtures exist and are documented.
- A developer can write a test that loads weather + generation sample Parquet and runs a simple DuckDB join in < 1s.
- No tests write to the repo’s real `data/` directory.
