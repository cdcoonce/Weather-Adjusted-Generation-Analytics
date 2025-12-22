# Phase 2.3 — Sample Parquet Data

## Objective
Create tiny committed Parquet files in `tests/data/` that represent the “happy path” for ingestion logic.

## Why commit data files?
- Improves reproducibility and makes tests independent of local mock-data generation.
- Prevents “works on my machine” differences due to timezones, random seeds, or library versions.

## Proposed files
- `tests/data/weather_2023-01-01.parquet`
- `tests/data/generation_2023-01-01.parquet`

## Schema requirements
Match the minimal columns used in code:
- Weather:
  - `timestamp`, `asset_id`, `wind_speed_mps`, `ghi`
- Generation:
  - `timestamp`, `asset_id`, `net_generation_mwh`, `asset_capacity_mw`

Optional: include the additional fields from README to future-proof.

## Size requirements
- ~48–200 rows total per dataset (e.g., 24 hours × 2 assets = 48 rows).
- Keep files small (< ~200KB each).

## How to generate (recommended)
- Use the Phase 2 Polars factories (02 doc) to generate the DataFrames.
- Write them to Parquet with deterministic dtypes.

## Validation checklist
- Reading them with `polars.read_parquet` works.
- The `timestamp` column is a proper datetime type.
- `asset_id` values match across both datasets so joins succeed.

## Acceptance criteria
- Parquet sample files exist and are committed.
- Any new developer can run unit tests without generating mock data.
