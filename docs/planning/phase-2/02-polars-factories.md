# Phase 2.2 — Polars Factories

## Objective
Provide small deterministic Polars DataFrame factories that cover the columns used by:
- loaders (`weather_loader.py`, `generation_loader.py`)
- correlation asset query (wind_speed_mps, ghi, net_generation_mwh, asset_capacity_mw)
- future dbt sources

## Where code should live
- `tests/fixtures/polars_factories.py`

## Proposed factories

### `weather_df_small(...)`
- Columns to include (minimal useful subset):
  - `timestamp` (datetime)
  - `asset_id` (string)
  - `wind_speed_mps` (float)
  - `ghi` (float)
  - Optionally: `temperature_c`, `pressure_hpa`, `relative_humidity`

### `generation_df_small(...)`
- Columns to include:
  - `timestamp` (datetime)
  - `asset_id` (string)
  - `gross_generation_mwh` (float)
  - `net_generation_mwh` (float)
  - `curtailment_mwh` (float)
  - `availability_pct` (float)
  - `asset_capacity_mw` (float)

## Determinism requirements
- Use a fixed date range by default (e.g., 24 hours) and fixed asset IDs (e.g., `asset_001`, `asset_002`).
- Avoid randomness unless you pass an explicit seed.
- Ensure values are simple and monotonic where possible (easy to assert).

## Recommended parameters
Factories should accept:
- `start: datetime`
- `periods: int`
- `freq: str` (hourly default)
- `asset_ids: list[str]`

## Acceptance criteria
- Factory output has stable schema and values.
- Data includes at least two assets and at least a few timestamps.
- Factories return eagerly evaluated `polars.DataFrame`.
