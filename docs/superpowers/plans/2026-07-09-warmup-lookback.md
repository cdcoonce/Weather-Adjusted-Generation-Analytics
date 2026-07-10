# Warm-up Lookback for Partitioned Warehouse Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the daily-partitioned Snowflake ingestion path physically plausible for state-carrying assets by simulating each partition with a 7-day warm-up window and per-day weather seeding.

**Architecture:** `synthetic_weather` becomes per-calendar-day seeded (`base_seed + day.toordinal()`) so any window is self-consistent; `simulate_fleet` gains `warmup_days` (simulate `[start − K days, end]`, return only `[start, end]`) and a separate `weather_seed` so weather stays constant-seeded while physics stays per-partition-seeded. The generation ingestion asset passes `warmup_days=7`; the weather ingestion asset drops its per-partition seed.

**Tech Stack:** Python 3.11+, Polars, NumPy, Dagster, dlt, pytest (markers: `unit`), uv, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-07-09-warmup-lookback-design.md`

## Global Constraints

- Run everything through `uv`: `uv run python -m pytest -m unit -q`, `uv run ruff check src/weather_analytics --select F,E9,I` (the CI variant), `uv run mypy src/weather_analytics/mock_data`.
- All new tests carry `pytestmark = pytest.mark.unit` (module level) or `@pytest.mark.unit`.
- Docstrings are numpydoc style, `from __future__ import annotations` at top of every module (already present in all touched files).
- Branch `fix/warmup-lookback-warehouse-path` cut from **fresh** `origin/main` (`git fetch origin && git checkout -b fix/warmup-lookback-warehouse-path origin/main`).
- No agent attribution in commit messages or PR bodies.
- Conventional commit style (`feat:`, `fix:`, `test:`, `docs:`) matching repo history.
- Do not change dbt models, RAW schema, or dashboard code — this is generator/asset-layer only.

---

### Task 1: Per-day weather seeding in `synthetic_weather`

**Files:**
- Modify: `src/weather_analytics/mock_data/weather_sources.py:125-224` (`synthetic_weather`)
- Test: `tests/unit/test_weather_sources.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `synthetic_weather(assets, start_date, end_date, random_seed=42) -> pl.DataFrame` — same signature, but `random_seed` is now a **base seed**; each calendar day is generated with `np.random.default_rng(random_seed + day.toordinal())` over the full 24h, then filtered to `[start_date, end_date]`. Also a private helper `_synthetic_weather_span(assets, timestamps, rng) -> pl.DataFrame`.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_weather_sources.py` (module already has `pytestmark = pytest.mark.unit`, imports `FLEET`, `pl`, and `weather_sources`):

```python
def test_synthetic_weather_day_slice_matches_standalone_day() -> None:
    """A day inside a multi-day window equals the same day generated alone."""
    from datetime import datetime

    window = weather_sources.synthetic_weather(
        FLEET, "2023-06-08T00:00:00", "2023-06-15T23:00:00", random_seed=42
    )
    day = weather_sources.synthetic_weather(
        FLEET, "2023-06-15T00:00:00", "2023-06-15T23:00:00", random_seed=42
    )
    sliced = window.filter(pl.col("timestamp") >= datetime(2023, 6, 15))
    assert sliced.equals(day)


def test_synthetic_weather_partial_day_window_consistent() -> None:
    """Partial-day windows reproduce the same values as full-day windows."""
    from datetime import datetime

    full = weather_sources.synthetic_weather(
        FLEET, "2023-06-15T00:00:00", "2023-06-15T23:00:00", random_seed=42
    )
    partial = weather_sources.synthetic_weather(
        FLEET, "2023-06-15T06:00:00", "2023-06-15T18:00:00", random_seed=42
    )
    expected = full.filter(
        (pl.col("timestamp") >= datetime(2023, 6, 15, 6))
        & (pl.col("timestamp") <= datetime(2023, 6, 15, 18))
    )
    assert partial.equals(expected)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run python -m pytest tests/unit/test_weather_sources.py -q`
Expected: the two new tests FAIL (current implementation seeds one rng for the whole window, so slices differ); existing tests PASS.

- [ ] **Step 3: Implement per-day generation**

In `weather_sources.py`, change the datetime import to `from datetime import datetime, timedelta`. Extract the current per-asset body of `synthetic_weather` (everything from `hours = ...` through the final `pl.concat`) into a helper that takes the time axis and rng, then rewrite `synthetic_weather` to loop calendar days:

```python
def _synthetic_weather_span(
    assets: tuple[FleetAsset, ...] | list[FleetAsset],
    timestamps: pl.Series,
    rng: np.random.Generator,
) -> pl.DataFrame:
    """Synthetic weather for all assets over one contiguous time axis."""
    hours = timestamps.dt.hour().to_numpy()
    doy = timestamps.dt.ordinal_day().to_numpy()
    n = len(timestamps)

    frames: list[pl.DataFrame] = []
    for asset in assets:
        # ... existing per-asset body, moved verbatim (declination, cloud,
        # ghi, wind, temp, pressure, humidity, frame append) ...
        ...
    return pl.concat(frames)


def synthetic_weather(
    assets: tuple[FleetAsset, ...] | list[FleetAsset],
    start_date: str,
    end_date: str,
    random_seed: int = 42,
) -> pl.DataFrame:
    """Latitude-aware synthetic hourly weather (offline fallback).

    Each calendar day is generated independently with seed
    ``random_seed + day.toordinal()`` and the result filtered to the requested
    window, so a given day's weather is identical no matter which window it
    appears in (warm-up day or its own partition).
    """
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)

    day_frames: list[pl.DataFrame] = []
    day = start_dt.date()
    while day <= end_dt.date():
        day_start = datetime(day.year, day.month, day.day)
        day_ts = pl.datetime_range(
            start=day_start,
            end=day_start + timedelta(hours=23),
            interval="1h",
            eager=True,
        )
        rng = np.random.default_rng(random_seed + day.toordinal())
        day_frames.append(_synthetic_weather_span(assets, day_ts, rng))
        day += timedelta(days=1)

    return (
        pl.concat(day_frames)
        .filter(
            (pl.col("timestamp") >= start_dt) & (pl.col("timestamp") <= end_dt)
        )
        .select(WEATHER_COLUMNS)
        .sort(["timestamp", "asset_id"])
    )
```

The `# ... existing per-asset body ...` marker means: move lines 152–223 of the current file (the `for asset in assets:` loop, minus the final `.select(...).sort(...)` which stays in `synthetic_weather`) into the helper unchanged. Keep the numpydoc Parameters/Returns sections of `synthetic_weather`, adding the per-day-seed sentence above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run python -m pytest tests/unit/test_weather_sources.py -q`
Expected: ALL PASS (including `test_synthetic_weather_is_deterministic` and schema tests — determinism and schema are preserved).

- [ ] **Step 5: Commit**

```bash
git add src/weather_analytics/mock_data/weather_sources.py tests/unit/test_weather_sources.py
git commit -m "feat: seed synthetic weather per calendar day for window-consistent generation"
```

---

### Task 2: `warmup_days` + `weather_seed` in `simulate_fleet`

**Files:**
- Modify: `src/weather_analytics/mock_data/simulate.py:128-152` (signature + weather call) and `:270-275` (return path)
- Test: `tests/unit/test_fleet_simulation.py`

**Interfaces:**
- Consumes: per-day-seeded `synthetic_weather` (Task 1) via `get_weather`.
- Produces: `simulate_fleet(start_date, end_date, assets=FLEET, use_real_weather=True, random_seed=42, warmup_days=0, weather_seed=42) -> SimulationResult`. With `warmup_days=K`, the simulation covers `[start − K days, end]` and both `result.generation` and `result.weather` are filtered back to `[start_date, end_date]`. `weather_seed` (not `random_seed`) feeds `get_weather`; `random_seed` seeds only the stochastic physics rng.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_fleet_simulation.py` (check its existing imports; it imports `simulate_fleet` and `FLEET` — reuse them):

```python
def test_warmup_equals_wide_window_filtered() -> None:
    """warmup_days=K is exactly a K-day-earlier window filtered to [start, end]."""
    from datetime import datetime

    warm = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        random_seed=7,
        warmup_days=7,
    )
    wide = simulate_fleet(
        "2023-06-08T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        random_seed=7,
    )
    expected = wide.generation.filter(
        pl.col("timestamp") >= datetime(2023, 6, 15)
    )
    assert warm.generation.equals(expected)


def test_warmup_output_bounded_to_requested_window() -> None:
    from datetime import datetime

    result = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        warmup_days=7,
    )
    for frame in (result.generation, result.weather):
        assert frame["timestamp"].min() == datetime(2023, 6, 15, 0)
        assert frame["timestamp"].max() == datetime(2023, 6, 15, 23)


def test_warmup_battery_soc_not_reset_to_half() -> None:
    """With warm-up, the target day's first battery SOC is off the 50% cold start."""
    result = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        warmup_days=7,
    )
    first_soc = (
        result.generation.filter(pl.col("asset_type") == "battery")
        .sort(["asset_id", "timestamp"])
        .group_by("asset_id", maintain_order=True)
        .first()["soc_pct"]
    )
    assert all(abs(v - 50.0) > 0.5 for v in first_soc)


def test_battery_net_negative_over_multiday_window() -> None:
    """Round-trip + parasitic losses make battery net generation negative overall."""
    result = simulate_fleet(
        "2023-06-01T00:00:00",
        "2023-06-14T23:00:00",
        FLEET,
        use_real_weather=False,
    )
    battery_net = (
        result.generation.filter(pl.col("asset_type") == "battery")
        .select(pl.col("net_generation_mwh").sum())
        .item()
    )
    assert battery_net < 0.0


def test_weather_seed_independent_of_physics_seed() -> None:
    """Same weather_seed => identical weather even when random_seed differs."""
    a = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        random_seed=1,
    )
    b = simulate_fleet(
        "2023-06-15T00:00:00",
        "2023-06-15T23:00:00",
        FLEET,
        use_real_weather=False,
        random_seed=2,
    )
    assert a.weather.equals(b.weather)
    assert not a.generation.equals(b.generation)
```

Note: `test_warmup_battery_soc_not_reset_to_half` asserts a physical outcome, not an exact value. If a battery's SOC lands within 0.5 pct-points of 50.0 by coincidence, loosen only after confirming the trajectory is genuinely continuous (print the warm-up SOC series) — do not delete the test. Column names to use: `asset_type`, `soc_pct`, `net_generation_mwh` (see `GENERATION_COLUMNS` in `simulate.py`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run python -m pytest tests/unit/test_fleet_simulation.py -q`
Expected: new tests FAIL — `warmup_days`/unexpected keyword for the first three; `test_weather_seed_independent_of_physics_seed` fails because `random_seed` currently feeds the weather too. `test_battery_net_negative_over_multiday_window` may already PASS (it tests existing physics over a long window) — that is fine.

- [ ] **Step 3: Implement**

In `simulate.py`, add `timedelta` to the datetime import (`from datetime import datetime, timedelta` — check the current import line and extend it). Change the signature and body:

```python
def simulate_fleet(
    start_date: str,
    end_date: str,
    assets: tuple[FleetAsset, ...] = FLEET,
    use_real_weather: bool = True,
    random_seed: int = 42,
    warmup_days: int = 0,
    weather_seed: int = 42,
) -> SimulationResult:
```

Docstring additions (numpydoc):

```text
warmup_days : int
    Simulate this many extra days before ``start_date`` and discard them from
    the returned frames. Lets state-carrying assets (battery SOC) and the
    dispatch rank signal reach a realistic trajectory before the target window.
weather_seed : int
    Base seed for synthetic weather. Kept separate from ``random_seed`` so
    every caller shares one weather realization per calendar day regardless
    of their physics seed.
```

Body changes — at the top of the function:

```python
    start_dt = datetime.fromisoformat(start_date)
    sim_start = (start_dt - timedelta(days=warmup_days)).isoformat()

    rng = np.random.default_rng(random_seed)
    weather, source = get_weather(
        assets, sim_start, end_date, use_real_weather, weather_seed
    )
```

And at the return path, filter both frames back to the requested window:

```python
    generation = (
        pl.concat(rows).select(GENERATION_COLUMNS).sort(["timestamp", "asset_id"])
    )
    if warmup_days > 0:
        generation = generation.filter(pl.col("timestamp") >= start_dt)
        weather = weather.filter(pl.col("timestamp") >= start_dt)
    return SimulationResult(generation, weather, source)
```

Everything between (potentials, demand, battery/gas dispatch, curtailment) is untouched — it operates on the wider weather frame automatically.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run python -m pytest tests/unit/test_fleet_simulation.py -q`
Expected: ALL PASS. Also run `uv run python -m pytest tests/unit/test_dashboard_export.py tests/unit/test_correlation_asset.py -q` to confirm downstream consumers of `simulate_fleet`/`SimulationResult` are unaffected (defaults preserve behavior).
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/weather_analytics/mock_data/simulate.py tests/unit/test_fleet_simulation.py
git commit -m "feat: add warmup_days and weather_seed to simulate_fleet"
```

---

### Task 3: `warmup_days` passthrough in `generate_generation_data`

**Files:**
- Modify: `src/weather_analytics/mock_data/generate_generation.py:35-77`
- Test: `tests/unit/test_fleet_simulation.py` (or the file that currently tests `generate_generation_data` — check `grep -rln generate_generation_data tests/unit/`; add to that file)

**Interfaces:**
- Consumes: `simulate_fleet(..., warmup_days=..., weather_seed=42)` from Task 2.
- Produces: `generate_generation_data(start_date, end_date, asset_configs=None, random_seed=42, warmup_days=0) -> pl.DataFrame`.

- [ ] **Step 1: Write the failing test**

```python
def test_generate_generation_data_warmup_is_idempotent_and_bounded() -> None:
    from datetime import datetime

    from weather_analytics.mock_data.generate_generation import (
        generate_generation_data,
    )

    a = generate_generation_data(
        "2023-06-15T00:00:00", "2023-06-15T23:00:00", random_seed=99, warmup_days=7
    )
    b = generate_generation_data(
        "2023-06-15T00:00:00", "2023-06-15T23:00:00", random_seed=99, warmup_days=7
    )
    assert a.equals(b)
    assert a["timestamp"].min() == datetime(2023, 6, 15, 0)
    assert a["timestamp"].max() == datetime(2023, 6, 15, 23)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m pytest tests/unit/ -q -k warmup_is_idempotent`
Expected: FAIL with `TypeError: ... unexpected keyword argument 'warmup_days'`.

- [ ] **Step 3: Implement**

Add the parameter and pass it through:

```python
def generate_generation_data(
    start_date: str,
    end_date: str,
    asset_configs: dict[str, dict[str, float | str]] | None = None,  # noqa: ARG001
    random_seed: int = 42,
    warmup_days: int = 0,
) -> pl.DataFrame:
```

```python
    result = simulate_fleet(
        start_date,
        end_date,
        FLEET,
        use_real_weather=False,
        random_seed=random_seed,
        warmup_days=warmup_days,
    )
```

Docstring: add a `warmup_days` entry mirroring Task 2's wording. Also update the module docstring line "Weather here is synthetic (deterministic per seed)" to "Weather here is synthetic and seeded per calendar day (shared `weather_seed` base), so ingestion is reproducible and consistent with the weather ingestion asset by construction."

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run python -m pytest tests/unit/ -q -k warmup_is_idempotent`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/weather_analytics/mock_data/generate_generation.py tests/unit/
git commit -m "feat: expose warmup_days through generate_generation_data"
```

---

### Task 4: Ingestion asset call sites

**Files:**
- Modify: `src/weather_analytics/assets/ingestion/generation.py:24-27,86-93`
- Modify: `src/weather_analytics/assets/ingestion/weather.py:26-29,88-97`
- Test: `tests/unit/test_generation_ingestion.py`, `tests/unit/test_weather_ingestion.py`

**Interfaces:**
- Consumes: `generate_generation_data(..., warmup_days=...)` (Task 3); per-day-seeded `generate_weather_data` (Task 1 via `synthetic_weather`).
- Produces: module constant `WARMUP_DAYS = 7` in `assets/ingestion/generation.py`. `waga_weather_ingestion` no longer passes `random_seed`.

- [ ] **Step 1: Write the failing tests**

In `tests/unit/test_generation_ingestion.py` (it already patches `generate_generation_data` and uses `build_asset_context` — follow the existing test at line ~130 for the fixture pattern):

```python
@pytest.mark.unit
def test_generation_ingestion_passes_warmup_and_partition_seed() -> None:
    """The asset requests a 7-day warm-up with the per-partition physics seed."""
    from datetime import date

    fake_df = pl.DataFrame(
        {"asset_id": ["ASSET_001"], "timestamp": [datetime(2023, 6, 15)]}
    )
    with patch(
        "weather_analytics.assets.ingestion.generation.generate_generation_data",
        return_value=fake_df,
    ) as gen:
        context = build_asset_context(partition_key="2023-06-15")
        fake_dlt_resource = MagicMock()
        fake_dlt_resource.get_pipeline.return_value = MagicMock()
        waga_generation_ingestion(context=context, dlt_ingestion=fake_dlt_resource)

    kwargs = gen.call_args.kwargs
    assert kwargs["warmup_days"] == 7
    assert kwargs["random_seed"] == date(2023, 6, 15).toordinal()
```

(Adjust the `fake_dlt_resource` wiring to match how the existing passing test in that file fakes the dlt pipeline — copy its arrangement verbatim; the new assertions are the `call_args.kwargs` block. Add `from datetime import datetime` to imports if missing.)

In `tests/unit/test_weather_ingestion.py`:

```python
@pytest.mark.unit
def test_weather_ingestion_uses_default_weather_seed() -> None:
    """The weather asset must NOT pass a per-partition seed (per-day seeding
    inside synthetic_weather keeps it consistent with generation)."""
    fake_df = pl.DataFrame(
        {"asset_id": ["ASSET_001"], "timestamp": [datetime(2023, 6, 15)]}
    )
    with patch(
        "weather_analytics.assets.ingestion.weather.generate_weather_data",
        return_value=fake_df,
    ) as gen:
        context = build_asset_context(partition_key="2023-06-15")
        fake_dlt_resource = MagicMock()
        fake_dlt_resource.get_pipeline.return_value = MagicMock()
        waga_weather_ingestion(context=context, dlt_ingestion=fake_dlt_resource)

    assert "random_seed" not in gen.call_args.kwargs
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run python -m pytest tests/unit/test_generation_ingestion.py tests/unit/test_weather_ingestion.py -q`
Expected: the two new tests FAIL (`warmup_days` missing; `random_seed` present). Existing tests PASS.

- [ ] **Step 3: Implement call-site changes**

`assets/ingestion/generation.py` — add below `GENERATION_PARTITIONS`:

```python
# Warm-up lookback: simulate this many days before the partition so battery
# SOC and the dispatch rank signal reach a realistic trajectory (see
# docs/superpowers/specs/2026-07-09-warmup-lookback-design.md).
WARMUP_DAYS = 7
```

and change the call:

```python
    df = generate_generation_data(
        start_date=start,
        end_date=end,
        random_seed=_partition_seed(partition_key),
        warmup_days=WARMUP_DAYS,
    )
```

Update `_partition_seed`'s docstring — it no longer keeps weather consistent, only physics idempotency:

```python
def _partition_seed(partition_key: str) -> int:
    """Deterministic per-day physics seed so re-runs merge idempotently.

    Weather consistency with the weather ingestion asset no longer depends on
    this seed — synthetic weather is seeded per calendar day internally.
    """
    return date.fromisoformat(partition_key).toordinal()
```

`assets/ingestion/weather.py` — delete `_partition_seed` (lines 26–29) and the now-unused `from datetime import date` import if nothing else uses it; change the call:

```python
    df = generate_weather_data(
        start_date=start,
        end_date=end,
        asset_count=WEATHER_ASSET_COUNT,
    )
```

Also update `generate_weather_data`'s `random_seed` docstring in `src/weather_analytics/mock_data/generate_weather.py` — replace "(must match the generation asset's seed to keep weather and generation consistent for the same partition)" with "Base seed for the per-calendar-day weather scheme; leave at the default so all callers share one weather realization." And update that module's docstring sentence "Deterministic per seed so the weather and generation ingestion assets stay consistent for a given partition." to "Seeded per calendar day from a shared base so the weather and generation ingestion assets stay consistent by construction."

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run python -m pytest tests/unit/test_generation_ingestion.py tests/unit/test_weather_ingestion.py -q`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add src/weather_analytics/assets/ingestion/ src/weather_analytics/mock_data/generate_weather.py tests/unit/test_generation_ingestion.py tests/unit/test_weather_ingestion.py
git commit -m "feat: ingest with 7-day warm-up window; drop per-partition weather seed"
```

---

### Task 5: Full gate + PR

**Files:**
- No new files; runs the repo gate and opens the PR.

- [ ] **Step 1: Run the full local gate**

```bash
uv run ruff check src/weather_analytics --select F,E9,I
uv run ruff check src/weather_analytics tests
uv run mypy src/weather_analytics/mock_data src/weather_analytics/assets
uv run python -m pytest -m unit -q
```

Expected: ruff clean, mypy clean on touched packages (pre-existing errors elsewhere are out of scope — compare against `origin/main` if any appear), all unit tests pass (~155+, was 146 + new).

- [ ] **Step 2: Push and open PR**

```bash
git push -u origin fix/warmup-lookback-warehouse-path
gh pr create --title "fix: warm-up lookback for partitioned warehouse path" --body "..."
```

PR body: summarize the spec (link `docs/superpowers/specs/2026-07-09-warmup-lookback-design.md`), the expected observable outcome (battery score ~88, not clamped 100), and the note that the first re-materialization rewrites RAW rows via dlt merge. No agent attribution.

- [ ] **Step 3: Watch CI to green, then merge**

```bash
gh pr checks --watch
gh pr merge --squash --delete-branch
```

Expected: Lint (ruff) + unit test jobs green **before** merging. Never merge red.

---

### Task 6: Live Snowflake verification (post-merge, mirrors 2026-07-06 run)

**Files:**
- No code changes. Requires Snowflake access (key-pair auth via `WAGASnowflakeResource`).

- [ ] **Step 1: Re-materialize a recent window of partitions**

From the repo root on `main` (post-merge), re-backfill ~5 recent days (each partition now simulates its own 7-day warm-up; no ordering dependency, any subset works):

```bash
for d in 2026-07-04 2026-07-05 2026-07-06 2026-07-07 2026-07-08; do
  uv run dagster asset materialize -m weather_analytics.definitions \
    --select waga_generation_ingestion --partition "$d"
  uv run dagster asset materialize -m weather_analytics.definitions \
    --select waga_weather_ingestion --partition "$d"
done
```

(Confirm the exact `-m`/`--select` invocation against the 2026-07-06 backfill commands in `docs/` or shell history if this form errors.)

- [ ] **Step 2: Run dbt build**

```bash
cd dbt_project && uv run dbt build
```

Expected: all models + tests PASS (92 checks as of 2026-07-06), contract enforcement green.

- [ ] **Step 3: Verify the battery score**

Query `mart_asset_weather_performance` for the re-materialized days:

```sql
select asset_id, avg(performance_score) as avg_score,
       avg(daily_capacity_factor) as avg_cf
from marts.mart_asset_weather_performance
where asset_type = 'battery'
  and performance_date between '2026-07-04' and '2026-07-08'
group by asset_id;
```

Expected: `avg_cf` slightly negative; `avg_score` near ~88 (not clamped at 100). Column names may differ (`performance_date` vs `date`) — check `mart_asset_weather_performance.sql` for exact names before running.

- [ ] **Step 4: Record the outcome**

Update the vault note `personal/projects/waga.md` (Current Work section) and the [[Gotchas]] battery-SOC nuance entry: the per-day SOC reset caveat is resolved by the warm-up window; warehouse battery scores now track the continuous reference.
