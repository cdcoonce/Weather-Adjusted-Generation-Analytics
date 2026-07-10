# Warm-up Lookback for the Partitioned Warehouse Path — Design

**Date:** 2026-07-09
**Status:** Approved
**Goal:** Make the daily-partitioned Snowflake ingestion path physically plausible for
state-carrying assets (battery SOC, dispatch percentiles) without requiring exact parity
with the continuous local simulation.

## Problem

Each daily partition of `waga_generation_ingestion` calls
`generate_generation_data(start, end)` with a single 24-hour window
(`assets/ingestion/generation.py`). Inside `simulate_fleet`:

- Battery SOC starts at 50% every call (`physics.py` — `soc = 0.5 * energy_max`), so the
  warehouse path resets battery state daily. Observed effect: battery daily capacity
  factor ≈ +0.03 and a mart performance score clamped to 100, versus the continuous
  local simulation's CF ≈ −1.6% and score ≈ 88.
- The battery/gas dispatch signal is a percentile rank of net load computed **within the
  window** (`simulate.py` — `_rank_signal`). With a 24h window, "cheap" and "expensive"
  hours are ranked against that day alone, so dispatch thresholds are unstable and
  unrealistic.

The local export path (`local_export.py`) simulates the whole date range continuously and
is the physically-accurate reference; it remains the source for the deployed portfolio
dashboard. This design fixes the warehouse path's realism, not parity.

## Design

### 1. `warmup_days` parameter

`simulate_fleet` and `generate_generation_data` gain `warmup_days: int = 0`. When set,
the simulation runs over `[start − warmup_days, end]` and the returned frame is filtered
to `[start, end]`.

The ingestion asset passes `warmup_days=7`:

- A 4h-duration battery cycling daily forgets its 50% initial condition within 1–2 days;
  by the target day SOC is on a realistic trajectory.
- `_rank_signal` normalizes over 8 days of net load instead of 1, stabilizing
  charge/discharge percentiles.

Default `0` preserves current behavior for all other callers (local export path is
unaffected — it already simulates continuously).

### 2. Per-day weather seeding in `synthetic_weather`

RAW.weather (written by `waga_weather_ingestion`) must stay identical to the weather
driving the generation physics — dbt marts and the correlation analysis join the two.
Today both assets pass the same per-partition seed (`_partition_seed` =
`date.toordinal()`), which breaks under warm-up windows: partition N's window would
regenerate days N−7…N−1 with seed f(N), not the seeds those days' own partitions used.

Fix: `synthetic_weather` generates each calendar day independently with
`seed = base_seed + day.toordinal()` and concatenates. Any window over any range is then
self-consistent — day N's weather is identical whether it appears as a warm-up day in a
later partition or as its own partition — **provided every caller uses the same
`base_seed`**. This forces a seed split in `simulate_fleet`: today one `random_seed`
feeds both weather and physics; after this change `simulate_fleet` (and
`generate_generation_data`) take a separate `weather_seed: int = 42` for the weather
base, while `random_seed` continues to seed the stochastic physics. Callers leave
`weather_seed` at its default.

Consequences:

- `waga_weather_ingestion` uses the default `weather_seed`; its `_partition_seed` helper
  is removed. `waga_generation_ingestion` keeps its per-partition seed, which now feeds
  only the stochastic physics (section 3) — weather derives from the shared per-day
  scheme, so the two assets stay consistent by construction.
- Sequential (AR-style) weather noise resets at day boundaries. This matches current
  behavior — each partition already generates its day in isolation — so intra-day realism
  is unchanged.
- Historical RAW rows regenerate identically only for re-runs under the new scheme; the
  first re-materialization after this change rewrites rows (dlt merge on
  `(asset_id, timestamp)` handles it, same as any generator change).

### 3. Physics noise seeding

Stochastic physics (turbulence AR(1), availability draws in `simulate.py`) stays seeded
once per call via `random_seed` — the per-partition `toordinal` seed, unchanged from
today — over the full warm-up window. It is not stored anywhere else, so it needs
idempotency only, not cross-window consistency. Same partition + same `warmup_days` →
byte-identical rows.

## Determinism and idempotency semantics

- Re-running a partition produces identical rows (dlt merge is a no-op update).
  Parallel backfills remain safe; no cross-partition dependencies are introduced.
- Changing `warmup_days` changes generated history on re-materialization. Accepted and
  documented; `warmup_days` is a code-level constant, not runtime config.
- The earliest partitions (from 2023-01-01) warm up into late-2022 dates; the synthetic
  generator handles arbitrary dates.

## What does not change

- No RAW/staging/marts schema changes; no dbt model or test changes. Battery daily CF
  going slightly negative is already accommodated (battery is excluded from the
  `daily_capacity_factor [0, 1.1]` range test).
- No dashboard or export changes. The local continuous sim remains the dashboard source.
- Ingestion cost per partition grows from 1 to 8 simulated days — fixed O(K), trivial at
  this fleet size.

## Expected observable outcome

After re-backfilling, warehouse battery daily CF is slightly negative (round-trip +
parasitic losses) and `mart_asset_weather_performance` battery score lands near the
continuous reference (~88), no longer clamped at 100. Wind/solar/gas rows for a given
day change only through the new per-day weather seeds, remaining physically equivalent.

## Testing

Unit (offline, deterministic):

1. **Weather slice consistency** — `synthetic_weather` over an 8-day window, sliced to
   the final day, equals `synthetic_weather` over that day alone (same base seed).
2. **Initial-condition washout** — target-day battery rows from a 7-day vs an 8-day
   warm-up agree within tolerance (SOC trajectory has forgotten the window start).
3. **Round-trip losses visible** — battery net generation summed over a multi-day
   warm-up window is negative.
4. **Idempotency** — two identical `generate_generation_data(..., warmup_days=7)` calls
   return identical frames.
5. **Default unchanged** — `warmup_days=0` reproduces current single-day behavior.

Gate: ruff, mypy on touched modules, full unit suite.

Live verification (mirrors 2026-07-06 run): re-backfill a handful of recent partitions
via `dagster asset materialize`, run `dbt build`, confirm all tests pass and the battery
performance score lands ~88 (not 100).

## Out of scope

- Freshness/anomaly asset checks and incremental dbt models (separate correctness
  follow-ups).
- Dashboard issue #33 (battery SOC / solar chart dispatch display, y-axis units).
- Exact parity between warehouse and continuous local simulation.
