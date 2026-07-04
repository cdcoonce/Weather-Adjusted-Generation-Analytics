# WAGA Static Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace WAGA's Panel/Bokeh/Pyodide dashboard with a standalone, server-rendered static dashboard (afk-cockpit style) deployed to its own Cloudflare Pages project, with light client-side filtering.

**Architecture:** A new standalone package `src/weather_analytics/cockpit/` reads the 4 JSON exports the existing Dagster asset `waga_dashboard_export_build` already writes to `dashboard_exports/`, computes KPI + inline-SVG geometry in pure Python (no chart library), renders one self-contained `dist/index.html` via Jinja2 (embedding the full dataset as a JSON island + inlined `app.js` for client-side filter/redraw), and deploys it with `npx wrangler pages deploy`. The daily launchd chain runs `cockpit build` + `cockpit deploy` after the Dagster export step. The old Pyodide dashboard, its publish asset, build/push scripts, and CI workflow are removed.

**Tech Stack:** Python 3 (stdlib `json` + `dataclasses`, no polars), Jinja2, vanilla JS (no framework), `wrangler` (via `npx`), pytest, ruff, `uv`.

## Global Constraints

- **Package under** `src/weather_analytics/cockpit/`; invoked as `python -m weather_analytics.cockpit <cmd>` (requires `__main__.py`).
- **No chart library, no Bokeh, no Pyodide, no CDN chart JS.** All chart geometry is inline SVG generated in Python; the one client-side script is hand-written vanilla JS inlined into the HTML. Output `dist/index.html` must be a single self-contained file (fonts-from-CDN `<link>` is acceptable; no other external assets).
- **New runtime dep:** `jinja2>=3.1` added to `[project.dependencies]`. No polars in the cockpit package.
- **Cloudflare project name:** `waga-dashboard` → production URL `https://waga-dashboard.pages.dev`. Deploy command mirrors afk-cockpit exactly: `npx --yes wrangler pages deploy <dir> --project-name waga-dashboard --branch main --commit-dirty=true`. `wrangler` reads `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID` from the environment.
- **Data contract (exact fields), produced by `waga_dashboard_export_build`, NaN→`null`, dates→ISO strings:**
  - `manifest.json`: `generated_at`, `pipeline_run_id`, `date_range:{start,end}`, `asset_count`, `row_counts:{daily_performance,weather_performance}`, `schema_version`.
  - `assets.json`: array of `{asset_id, capacity_mw, size_category, asset_type (wind|solar), display_name}`.
  - `daily_performance.json`: array, 15 fields: `asset_id, date, total_net_generation_mwh, daily_capacity_factor, avg_availability_pct, total_curtailment_mwh, daily_performance_rating, excellent_hours, good_hours, fair_hours, poor_hours, avg_wind_speed_mps, avg_ghi, avg_temperature_c, data_completeness_pct`.
  - `weather_performance.json`: array, 12 fields: `asset_id, date, performance_score, performance_category, avg_expected_generation_mwh, avg_actual_generation_mwh, avg_performance_ratio_pct, wind_r_squared, solar_r_squared, inferred_asset_type, rolling_7d_avg_cf, rolling_30d_avg_cf`.
- **Interactivity model (approved spec):** default view is server-rendered (works with JS off). `app.js` adds asset-type / individual-asset filter + date-range toggle that recompute KPIs and redraw the SVG charts from the JSON island. The KPI/geometry math in `app.js` mirrors the pure Python functions in `charts.py`; the Python functions take optional filter args so pytest verifies the reference math.
- **Branch prerequisite:** the Task 8 edit to `scripts/run_scheduled.py` exists only on `feat/local-launchd-scheduling`. The implementation branch MUST be based on that branch (recommended: merge `feat/local-launchd-scheduling` to `main` first, then branch from `main`). Do not start Task 8 on a branch lacking `scripts/run_scheduled.py`.
- **Lint is strict.** WAGA's ruff `select` includes `ANN` (type-annotation), `T20` (no `print`), `INP`, `PL`, `TRY`, `EM`, etc., and the only relevant `per-file-ignores` are `tests/* = ["ANN","ARG","S101"]` and `scripts/* = ["T201","INP001"]` — the cockpit package gets the **full** rule set. Therefore: annotate **every** function parameter and return (ANN001); Task 1 adds a `src/weather_analytics/cockpit/* = ["T201"]` per-file-ignore so the CLI/serve `print()`s pass. After each task, run the task's `ruff check`; resolve any residual finding with the narrowest fix (add the missing annotation, or a targeted `# noqa: <CODE>` with a one-word reason) — never leave the gate red.
- **Commit style:** Conventional Commits, scope `dashboard` or `cockpit` (e.g. `feat(cockpit): ...`, `chore(dashboard): ...`).
- **Verify commands:** `uv run pytest tests/cockpit/ -v` (package tests); `uv run ruff check src/weather_analytics/cockpit tests/cockpit`; `uv run dagster definitions validate` (after Task 8); portfolio repo uses its own `npm test`.

## Pre-execution checklist (gated — Charles does these; not code tasks)

- [ ] Create a Cloudflare Pages project named `waga-dashboard` (Direct Upload) in the Cloudflare dashboard.
- [ ] Create a Cloudflare API token with the **Pages:Edit** permission; note the Account ID.
- [ ] Add `CLOUDFLARE_API_TOKEN=...` and `CLOUDFLARE_ACCOUNT_ID=...` to WAGA's gitignored `.env`.
- [ ] Confirm `feat/local-launchd-scheduling` is merged to `main` (or is the base of the implementation branch).

## File Structure

- `src/weather_analytics/cockpit/__init__.py` — package marker + version.
- `src/weather_analytics/cockpit/__main__.py` — `python -m` entry: `raise SystemExit(main())`.
- `src/weather_analytics/cockpit/config.py` — constants (default export dir, dist path, CF project name).
- `src/weather_analytics/cockpit/data.py` — dataclasses + `load_dataset()` (JSON → typed `Dataset`, also keeps `.raw`).
- `src/weather_analytics/cockpit/charts.py` — pure KPI + inline-SVG geometry functions (the client-side JS reference math).
- `src/weather_analytics/cockpit/render.py` — Jinja render → self-contained `dist/index.html`.
- `src/weather_analytics/cockpit/templates/index.html.j2` — the page (inline `<style>`, JSON island, `{{ app_js }}` script).
- `src/weather_analytics/cockpit/static/app.js` — client-side filter/redraw (read at build, inlined).
- `src/weather_analytics/cockpit/cloudflare.py` — `deploy()` (copied from afk-cockpit).
- `src/weather_analytics/cockpit/serve.py` — local static server for `dist/`.
- `src/weather_analytics/cockpit/cli.py` — argparse subcommands build/deploy/serve.
- `tests/cockpit/conftest.py` — fixtures (a trimmed 4-JSON dataset in `tests/cockpit/fixtures/`).
- `tests/cockpit/{test_data,test_charts,test_render,test_cloudflare,test_serve,test_cli}.py`.

---

### Task 1: Package scaffold + config + dependency

**Files:**
- Create: `src/weather_analytics/cockpit/__init__.py`
- Create: `src/weather_analytics/cockpit/config.py`
- Modify: `pyproject.toml` (add `jinja2>=3.1` to `[project.dependencies]`)
- Test: `tests/cockpit/test_config.py`

**Interfaces:**
- Produces: `config.DEFAULT_EXPORT_DIR: str = "dashboard_exports"`, `config.DEFAULT_DIST_DIR: str = "dist"`, `config.DEFAULT_OUT: str = "dist/index.html"`, `config.CF_PROJECT_NAME: str = "waga-dashboard"`, `config.SITE_URL: str = "https://waga-dashboard.pages.dev"`.

- [ ] **Step 1: Write the failing test**

```python
# tests/cockpit/test_config.py
from weather_analytics.cockpit import config


def test_config_constants():
    assert config.CF_PROJECT_NAME == "waga-dashboard"
    assert config.SITE_URL == "https://waga-dashboard.pages.dev"
    assert config.DEFAULT_EXPORT_DIR == "dashboard_exports"
    assert config.DEFAULT_OUT == "dist/index.html"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cockpit/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'weather_analytics.cockpit'`

- [ ] **Step 3: Create the package + config**

```python
# src/weather_analytics/cockpit/__init__.py
"""Standalone static dashboard for WAGA (afk-cockpit style).

Reads the 4 JSON exports written by the ``waga_dashboard_export_build`` Dagster
asset and renders a single self-contained ``dist/index.html`` deployed to
Cloudflare Pages. No Dagster context, no Snowflake, no chart library.
"""

__version__ = "0.1.0"
```

```python
# src/weather_analytics/cockpit/config.py
"""Cockpit constants: default paths and Cloudflare Pages target."""

from __future__ import annotations

DEFAULT_EXPORT_DIR = "dashboard_exports"
DEFAULT_DIST_DIR = "dist"
DEFAULT_OUT = "dist/index.html"
CF_PROJECT_NAME = "waga-dashboard"
SITE_URL = "https://waga-dashboard.pages.dev"
```

- [ ] **Step 4: Add jinja2 dependency**

In `pyproject.toml`, under `[project]` `dependencies = [`, add the line (keep alphabetical/style with siblings):

```toml
    "jinja2>=3.1",
```

Then sync: `uv sync`

- [ ] **Step 5: Add the cockpit per-file-ignore for `print`**

The cockpit CLI/serve legitimately call `print()` (like the repo's `scripts/*`), but the cockpit package is under `src/` so it gets `T20`. In `pyproject.toml` under `[tool.ruff.per-file-ignores]` (it already has `tests/*` and `scripts/*` entries), add:

```toml
"src/weather_analytics/cockpit/*" = ["T201"]  # CLI/serve print user-facing output
```

All other rules (ANN, etc.) stay enforced — the plan's code is fully annotated to satisfy them.

- [ ] **Step 6: Run tests + ruff**

Run: `uv run pytest tests/cockpit/test_config.py -v && uv run ruff check src/weather_analytics/cockpit`
Expected: PASS, no lint errors.

- [ ] **Step 7: Commit**

```bash
git add src/weather_analytics/cockpit/__init__.py src/weather_analytics/cockpit/config.py pyproject.toml tests/cockpit/test_config.py uv.lock
git commit -m "feat(cockpit): scaffold package, config, add jinja2 dep"
```

---

### Task 2: `data.py` — load & normalize the 4 JSON exports

**Files:**
- Create: `src/weather_analytics/cockpit/data.py`
- Create: `tests/cockpit/fixtures/{manifest,assets,daily_performance,weather_performance}.json`
- Create: `tests/cockpit/conftest.py`
- Test: `tests/cockpit/test_data.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) Asset(asset_id: str, capacity_mw: float, size_category: str, asset_type: str, display_name: str)`
  - `@dataclass(frozen=True) DailyRow(asset_id: str, date: str, total_net_generation_mwh: float, daily_capacity_factor: float, avg_availability_pct: float, total_curtailment_mwh: float, daily_performance_rating: str)`
  - `@dataclass(frozen=True) WeatherRow(asset_id: str, date: str, performance_score: float, performance_category: str, inferred_asset_type: str)`
  - `@dataclass(frozen=True) Manifest(generated_at: str, date_range_start: str, date_range_end: str, asset_count: int, schema_version: str)`
  - `@dataclass(frozen=True) Dataset(manifest: Manifest, assets: list[Asset], daily: list[DailyRow], weather: list[WeatherRow], raw: dict)`
  - `load_dataset(export_dir: Path) -> Dataset`
- Consumes: nothing (leaf module). `charts.py`, `render.py`, `cli.py` consume `Dataset` + `load_dataset`.

- [ ] **Step 1: Create the fixtures**

Create `tests/cockpit/fixtures/manifest.json`:

```json
{
  "generated_at": "2026-07-03T06:15:00Z",
  "pipeline_run_id": "run-abc-123",
  "date_range": {"start": "2026-07-01", "end": "2026-07-02"},
  "asset_count": 2,
  "row_counts": {"daily_performance": 4, "weather_performance": 4},
  "schema_version": "1.0"
}
```

Create `tests/cockpit/fixtures/assets.json`:

```json
[
  {"asset_id": "W1", "capacity_mw": 100.0, "size_category": "large", "asset_type": "wind", "display_name": "Wind Asset W1 (100.0 MW)"},
  {"asset_id": "S1", "capacity_mw": 50.0, "size_category": "medium", "asset_type": "solar", "display_name": "Solar Asset S1 (50.0 MW)"}
]
```

Create `tests/cockpit/fixtures/daily_performance.json`:

```json
[
  {"asset_id": "W1", "date": "2026-07-01", "total_net_generation_mwh": 800.0, "daily_capacity_factor": 0.33, "avg_availability_pct": 98.0, "total_curtailment_mwh": 10.0, "daily_performance_rating": "good", "excellent_hours": 4, "good_hours": 10, "fair_hours": 6, "poor_hours": 4, "avg_wind_speed_mps": 7.1, "avg_ghi": null, "avg_temperature_c": 18.0, "data_completeness_pct": 100.0},
  {"asset_id": "W1", "date": "2026-07-02", "total_net_generation_mwh": 900.0, "daily_capacity_factor": 0.38, "avg_availability_pct": 99.0, "total_curtailment_mwh": 5.0, "daily_performance_rating": "excellent", "excellent_hours": 8, "good_hours": 10, "fair_hours": 4, "poor_hours": 2, "avg_wind_speed_mps": 8.0, "avg_ghi": null, "avg_temperature_c": 19.0, "data_completeness_pct": 100.0},
  {"asset_id": "S1", "date": "2026-07-01", "total_net_generation_mwh": 300.0, "daily_capacity_factor": 0.25, "avg_availability_pct": 97.0, "total_curtailment_mwh": 0.0, "daily_performance_rating": "fair", "excellent_hours": 2, "good_hours": 6, "fair_hours": 10, "poor_hours": 6, "avg_wind_speed_mps": null, "avg_ghi": 5.2, "avg_temperature_c": 22.0, "data_completeness_pct": 100.0},
  {"asset_id": "S1", "date": "2026-07-02", "total_net_generation_mwh": 320.0, "daily_capacity_factor": 0.27, "avg_availability_pct": 98.0, "total_curtailment_mwh": 0.0, "daily_performance_rating": "good", "excellent_hours": 3, "good_hours": 8, "fair_hours": 9, "poor_hours": 4, "avg_wind_speed_mps": null, "avg_ghi": 5.6, "avg_temperature_c": 23.0, "data_completeness_pct": 100.0}
]
```

Create `tests/cockpit/fixtures/weather_performance.json`:

```json
[
  {"asset_id": "W1", "date": "2026-07-01", "performance_score": 0.92, "performance_category": "on_target", "avg_expected_generation_mwh": 850.0, "avg_actual_generation_mwh": 800.0, "avg_performance_ratio_pct": 94.0, "wind_r_squared": 0.81, "solar_r_squared": null, "inferred_asset_type": "wind", "rolling_7d_avg_cf": 0.34, "rolling_30d_avg_cf": 0.33},
  {"asset_id": "W1", "date": "2026-07-02", "performance_score": 0.97, "performance_category": "outperforming", "avg_expected_generation_mwh": 880.0, "avg_actual_generation_mwh": 900.0, "avg_performance_ratio_pct": 102.0, "wind_r_squared": 0.83, "solar_r_squared": null, "inferred_asset_type": "wind", "rolling_7d_avg_cf": 0.36, "rolling_30d_avg_cf": 0.34},
  {"asset_id": "S1", "date": "2026-07-01", "performance_score": 0.80, "performance_category": "underperforming", "avg_expected_generation_mwh": 360.0, "avg_actual_generation_mwh": 300.0, "avg_performance_ratio_pct": 83.0, "wind_r_squared": null, "solar_r_squared": 0.77, "inferred_asset_type": "solar", "rolling_7d_avg_cf": 0.26, "rolling_30d_avg_cf": 0.25},
  {"asset_id": "S1", "date": "2026-07-02", "performance_score": 0.85, "performance_category": "on_target", "avg_expected_generation_mwh": 355.0, "avg_actual_generation_mwh": 320.0, "avg_performance_ratio_pct": 90.0, "wind_r_squared": null, "solar_r_squared": 0.79, "inferred_asset_type": "solar", "rolling_7d_avg_cf": 0.27, "rolling_30d_avg_cf": 0.26}
]
```

- [ ] **Step 2: Add the conftest fixture**

```python
# tests/cockpit/conftest.py
from pathlib import Path

import pytest

from weather_analytics.cockpit.data import Dataset, load_dataset

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def dataset() -> Dataset:
    """The trimmed 2-asset x 2-day dataset from tests/cockpit/fixtures/."""
    return load_dataset(FIXTURES)
```

- [ ] **Step 3: Write the failing test**

```python
# tests/cockpit/test_data.py
from weather_analytics.cockpit.data import Dataset


def test_load_dataset_parses_all_four_files(dataset: Dataset):
    assert dataset.manifest.asset_count == 2
    assert dataset.manifest.date_range_start == "2026-07-01"
    assert dataset.manifest.date_range_end == "2026-07-02"
    assert {a.asset_id for a in dataset.assets} == {"W1", "S1"}
    assert len(dataset.daily) == 4
    assert len(dataset.weather) == 4


def test_asset_types_normalized(dataset: Dataset):
    by_id = {a.asset_id: a for a in dataset.assets}
    assert by_id["W1"].asset_type == "wind"
    assert by_id["S1"].asset_type == "solar"


def test_raw_holds_all_four_payloads(dataset: Dataset):
    assert set(dataset.raw) == {"manifest", "assets", "daily", "weather"}
    assert isinstance(dataset.raw["daily"], list)
    assert dataset.raw["manifest"]["schema_version"] == "1.0"
```

- [ ] **Step 4: Run test to verify it fails**

Run: `uv run pytest tests/cockpit/test_data.py -v`
Expected: FAIL — `ModuleNotFoundError` / `load_dataset` undefined.

- [ ] **Step 5: Implement `data.py`**

```python
# src/weather_analytics/cockpit/data.py
"""Load the 4 JSON exports into typed structures.

Pure stdlib. Unknown extra keys in the JSON are ignored; the loaders pull only
the fields the dashboard uses, but ``Dataset.raw`` keeps the full parsed
payloads for the client-side JSON island.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Asset:
    asset_id: str
    capacity_mw: float
    size_category: str
    asset_type: str
    display_name: str


@dataclass(frozen=True)
class DailyRow:
    asset_id: str
    date: str
    total_net_generation_mwh: float
    daily_capacity_factor: float
    avg_availability_pct: float
    total_curtailment_mwh: float
    daily_performance_rating: str


@dataclass(frozen=True)
class WeatherRow:
    asset_id: str
    date: str
    performance_score: float
    performance_category: str
    inferred_asset_type: str


@dataclass(frozen=True)
class Manifest:
    generated_at: str
    date_range_start: str
    date_range_end: str
    asset_count: int
    schema_version: str


@dataclass(frozen=True)
class Dataset:
    manifest: Manifest
    assets: list[Asset]
    daily: list[DailyRow]
    weather: list[WeatherRow]
    raw: dict


def _num(value: object, default: float = 0.0) -> float:
    """Coerce a JSON value to float, mapping null/None/missing to ``default``."""
    if value is None:
        return default
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def load_dataset(export_dir: Path) -> Dataset:
    """Read manifest/assets/daily_performance/weather_performance from export_dir."""
    export_dir = Path(export_dir)
    manifest_raw = _load_json(export_dir / "manifest.json")
    assets_raw = _load_json(export_dir / "assets.json")
    daily_raw = _load_json(export_dir / "daily_performance.json")
    weather_raw = _load_json(export_dir / "weather_performance.json")

    date_range = manifest_raw.get("date_range", {})
    manifest = Manifest(
        generated_at=str(manifest_raw.get("generated_at", "")),
        date_range_start=str(date_range.get("start", "")),
        date_range_end=str(date_range.get("end", "")),
        asset_count=int(manifest_raw.get("asset_count", 0)),
        schema_version=str(manifest_raw.get("schema_version", "")),
    )

    assets = [
        Asset(
            asset_id=str(a["asset_id"]),
            capacity_mw=_num(a.get("capacity_mw")),
            size_category=str(a.get("size_category", "")),
            asset_type=str(a.get("asset_type", "")),
            display_name=str(a.get("display_name", a.get("asset_id", ""))),
        )
        for a in assets_raw
    ]

    daily = [
        DailyRow(
            asset_id=str(r["asset_id"]),
            date=str(r["date"]),
            total_net_generation_mwh=_num(r.get("total_net_generation_mwh")),
            daily_capacity_factor=_num(r.get("daily_capacity_factor")),
            avg_availability_pct=_num(r.get("avg_availability_pct")),
            total_curtailment_mwh=_num(r.get("total_curtailment_mwh")),
            daily_performance_rating=str(r.get("daily_performance_rating", "")),
        )
        for r in daily_raw
    ]

    weather = [
        WeatherRow(
            asset_id=str(r["asset_id"]),
            date=str(r["date"]),
            performance_score=_num(r.get("performance_score")),
            performance_category=str(r.get("performance_category", "")),
            inferred_asset_type=str(r.get("inferred_asset_type", "")),
        )
        for r in weather_raw
    ]

    raw = {
        "manifest": manifest_raw,
        "assets": assets_raw,
        "daily": daily_raw,
        "weather": weather_raw,
    }
    return Dataset(manifest=manifest, assets=assets, daily=daily, weather=weather, raw=raw)
```

- [ ] **Step 6: Run tests + ruff**

Run: `uv run pytest tests/cockpit/test_data.py -v && uv run ruff check src/weather_analytics/cockpit/data.py`
Expected: PASS, no lint errors.

- [ ] **Step 7: Commit**

```bash
git add src/weather_analytics/cockpit/data.py tests/cockpit/
git commit -m "feat(cockpit): load and normalize the 4 JSON exports"
```

---

### Task 3: `charts.py` — pure KPI + inline-SVG geometry

**Files:**
- Create: `src/weather_analytics/cockpit/charts.py`
- Test: `tests/cockpit/test_charts.py`

**Interfaces:**
- Consumes: `data.Dataset`, `data.DailyRow`, `data.WeatherRow`, `data.Asset`.
- Produces (all pure; filters default to "all"):
  - `filter_daily(rows: list[DailyRow], asset_ids: set[str] | None, start: str | None, end: str | None) -> list[DailyRow]`
  - `filter_weather(rows: list[WeatherRow], asset_ids: set[str] | None, start: str | None, end: str | None) -> list[WeatherRow]`
  - `fleet_kpis(dataset: Dataset, asset_ids: set[str] | None = None, start: str | None = None, end: str | None = None) -> list[dict]` → each `{"key": str, "label": str, "value": str}` (keys: `capacity_factor`, `net_generation`, `performance_score`, `curtailment`).
  - `line_series(pairs: list[tuple[str, float]], width: int = 720, height: int = 200, pad: int = 8) -> dict | None` → `{"width","height","pad","area_path": str, "polyline": str, "y_max": float, "x0_label": str, "x1_label": str}` (SVG geometry; `None` if empty).
  - `generation_series(dataset, asset_ids=None, start=None, end=None) -> dict | None` (sum net-gen per date → `line_series`).
  - `capacity_factor_series(dataset, asset_ids=None, start=None, end=None) -> dict | None` (mean CF per date → `line_series`).
  - `performance_series(dataset, asset_ids=None, start=None, end=None) -> dict | None` (mean perf-score per date → `line_series`).
  - `asset_bars(dataset, asset_ids=None, start=None, end=None) -> list[dict]` → `[{"label","disp","pct","asset_type"}]` (mean CF per asset; `pct` = value/max*100, floor 1.5).
  - `type_split(dataset, asset_ids=None, start=None, end=None) -> list[dict]` → `[{"label","disp","pct"}]` (net-gen summed by wind vs solar).

- [ ] **Step 1: Write the failing test**

```python
# tests/cockpit/test_charts.py
from weather_analytics.cockpit import charts


def test_fleet_kpis_all(dataset):
    kpis = {k["key"]: k for k in charts.fleet_kpis(dataset)}
    # net generation = 800+900+300+320 = 2320 MWh
    assert "2,320" in kpis["net_generation"]["value"]
    # curtailment = 10+5+0+0 = 15 MWh
    assert "15" in kpis["curtailment"]["value"]
    # capacity factor = mean(0.33,0.38,0.25,0.27) = 0.3075 -> "30.8%" (pct, 1 dp)
    assert kpis["capacity_factor"]["value"].endswith("%")


def test_fleet_kpis_filtered_by_asset(dataset):
    kpis = {k["key"]: k for k in charts.fleet_kpis(dataset, asset_ids={"S1"})}
    # S1 only: net gen = 300+320 = 620
    assert "620" in kpis["net_generation"]["value"]


def test_generation_series_shape(dataset):
    s = charts.generation_series(dataset)
    assert s is not None
    assert s["polyline"]  # non-empty points string
    assert s["y_max"] > 0
    assert s["x0_label"] == "2026-07-01"
    assert s["x1_label"] == "2026-07-02"


def test_asset_bars_pct_of_max(dataset):
    bars = {b["label"]: b for b in charts.asset_bars(dataset)}
    # W1 mean CF = 0.355 (max), S1 mean CF = 0.26 -> W1 pct == 100
    assert bars["W1"]["pct"] == 100
    assert bars["W1"]["asset_type"] == "wind"
    assert bars["S1"]["pct"] < 100


def test_type_split_wind_vs_solar(dataset):
    split = {s["label"]: s for s in charts.type_split(dataset)}
    assert set(split) == {"wind", "solar"}
    # wind = 1700, solar = 620 -> wind is max
    assert split["wind"]["pct"] == 100


def test_empty_series_returns_none(dataset):
    assert charts.generation_series(dataset, start="2030-01-01") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cockpit/test_charts.py -v`
Expected: FAIL — `charts` functions undefined.

- [ ] **Step 3: Implement `charts.py`**

```python
# src/weather_analytics/cockpit/charts.py
"""Pure KPI + inline-SVG geometry. No chart library.

Every function takes the typed Dataset (plus optional asset/date filters) and
returns plain dicts/lists/strings — never a DataFrame. The client-side app.js
mirrors this math to redraw on filter changes; keep the two in sync.
"""

from __future__ import annotations

from statistics import fmean

from weather_analytics.cockpit.data import Dataset, DailyRow, WeatherRow


def _in_range(date: str, start: str | None, end: str | None) -> bool:
    if start is not None and date < start:
        return False
    return not (end is not None and date > end)


def filter_daily(
    rows: list[DailyRow],
    asset_ids: set[str] | None,
    start: str | None,
    end: str | None,
) -> list[DailyRow]:
    return [
        r
        for r in rows
        if (asset_ids is None or r.asset_id in asset_ids)
        and _in_range(r.date, start, end)
    ]


def filter_weather(
    rows: list[WeatherRow],
    asset_ids: set[str] | None,
    start: str | None,
    end: str | None,
) -> list[WeatherRow]:
    return [
        r
        for r in rows
        if (asset_ids is None or r.asset_id in asset_ids)
        and _in_range(r.date, start, end)
    ]


def fleet_kpis(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    weather = filter_weather(dataset.weather, asset_ids, start, end)
    net_gen = sum(r.total_net_generation_mwh for r in daily)
    curtailment = sum(r.total_curtailment_mwh for r in daily)
    cf = fmean([r.daily_capacity_factor for r in daily]) if daily else 0.0
    perf = fmean([r.performance_score for r in weather]) if weather else 0.0
    return [
        {"key": "capacity_factor", "label": "fleet capacity factor", "value": f"{cf * 100:.1f}%"},
        {"key": "net_generation", "label": "net generation (MWh)", "value": f"{net_gen:,.0f}"},
        {"key": "performance_score", "label": "avg weather-adj. score", "value": f"{perf:.2f}"},
        {"key": "curtailment", "label": "curtailment (MWh)", "value": f"{curtailment:,.0f}"},
    ]


def line_series(
    pairs: list[tuple[str, float]],
    width: int = 720,
    height: int = 200,
    pad: int = 8,
) -> dict | None:
    """Turn ordered (date, value) pairs into SVG polyline + filled-area geometry."""
    if not pairs:
        return None
    values = [v for _, v in pairs]
    y_max = max(values) or 1.0
    n = len(pairs)

    def x(i: int) -> float:
        return pad + (i / (n - 1) * (width - 2 * pad) if n > 1 else 0.0)

    def y(v: float) -> float:
        return height - pad - (v / y_max) * (height - 2 * pad)

    pts = [(x(i), y(v)) for i, v in enumerate(values)]
    polyline = " ".join(f"{px:.1f},{py:.1f}" for px, py in pts)
    area = (
        f"M {pts[0][0]:.1f} {height - pad:.1f} "
        + " ".join(f"L {px:.1f} {py:.1f}" for px, py in pts)
        + f" L {pts[-1][0]:.1f} {height - pad:.1f} Z"
    )
    return {
        "width": width,
        "height": height,
        "pad": pad,
        "area_path": area,
        "polyline": polyline,
        "y_max": y_max,
        "x0_label": pairs[0][0],
        "x1_label": pairs[-1][0],
    }


def _by_date_sum(rows: list[DailyRow], attr: str) -> list[tuple[str, float]]:
    acc: dict[str, float] = {}
    for r in rows:
        acc[r.date] = acc.get(r.date, 0.0) + getattr(r, attr)
    return sorted(acc.items())


def _by_date_mean(dates_values: list[tuple[str, float]]) -> list[tuple[str, float]]:
    groups: dict[str, list[float]] = {}
    for date, value in dates_values:
        groups.setdefault(date, []).append(value)
    return sorted((d, fmean(vs)) for d, vs in groups.items())


def generation_series(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict | None:
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    return line_series(_by_date_sum(daily, "total_net_generation_mwh"))


def capacity_factor_series(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict | None:
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    pairs = _by_date_mean([(r.date, r.daily_capacity_factor) for r in daily])
    return line_series(pairs, height=160)


def performance_series(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict | None:
    weather = filter_weather(dataset.weather, asset_ids, start, end)
    pairs = _by_date_mean([(r.date, r.performance_score) for r in weather])
    return line_series(pairs, height=160)


def _hbars(
    labels_values: list[tuple[str, float]], extra: dict | None = None
) -> list[dict]:
    max_v = max((v for _, v in labels_values), default=0.0)
    out: list[dict] = []
    for label, value in labels_values:
        row = {
            "label": label,
            "disp": f"{value:,.2f}",
            "pct": max(1.5, value / max_v * 100) if max_v else 0.0,
        }
        if extra and label in extra:
            row.update(extra[label])
        out.append(row)
    return out


def asset_bars(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    type_by_id = {a.asset_id: a.asset_type for a in dataset.assets}
    groups: dict[str, list[float]] = {}
    for r in daily:
        groups.setdefault(r.asset_id, []).append(r.daily_capacity_factor)
    means = sorted((aid, fmean(vs)) for aid, vs in groups.items())
    extra = {aid: {"asset_type": type_by_id.get(aid, "")} for aid, _ in means}
    return _hbars(means, extra)


def type_split(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    type_by_id = {a.asset_id: a.asset_type for a in dataset.assets}
    totals: dict[str, float] = {}
    for r in daily:
        t = type_by_id.get(r.asset_id, "unknown")
        totals[t] = totals.get(t, 0.0) + r.total_net_generation_mwh
    return _hbars(sorted(totals.items()))
```

- [ ] **Step 4: Run tests + ruff**

Run: `uv run pytest tests/cockpit/test_charts.py -v && uv run ruff check src/weather_analytics/cockpit/charts.py`
Expected: PASS, no lint errors.

- [ ] **Step 5: Commit**

```bash
git add src/weather_analytics/cockpit/charts.py tests/cockpit/test_charts.py
git commit -m "feat(cockpit): pure KPI and inline-SVG chart geometry"
```

---

### Task 4: `render.py` + template — self-contained `dist/index.html`

**Files:**
- Create: `src/weather_analytics/cockpit/render.py`
- Create: `src/weather_analytics/cockpit/templates/index.html.j2`
- Modify: `pyproject.toml` — ensure the package ships its `templates/` + `static/` data (hatch: add to wheel `force-include` / `artifacts`, see Step 5).
- Test: `tests/cockpit/test_render.py`

**Interfaces:**
- Consumes: `data.Dataset`, `charts.*`, `config.SITE_URL`.
- Produces: `render_dashboard(dataset: Dataset, out_path: Path, app_js: str = "") -> None` — writes a single self-contained HTML file. `app_js` is the inlined client script (empty string acceptable until Task 5).

- [ ] **Step 1: Write the failing test**

```python
# tests/cockpit/test_render.py
from html.parser import HTMLParser


from weather_analytics.cockpit.render import render_dashboard


def test_render_writes_self_contained_html(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)
    html = out.read_text(encoding="utf-8")
    low = html.lower()
    assert "<html" in low
    assert "weather-adjusted" in low  # title/heading present
    # no legacy chart runtimes:
    for banned in ("bokeh", "pyodide", "panel", "plotly"):
        assert banned not in low


def test_render_embeds_json_island(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)
    html = out.read_text(encoding="utf-8")
    assert 'id="cockpit-data"' in html
    assert 'type="application/json"' in html
    assert '"W1"' in html  # dataset serialized into the island


def test_render_inlines_app_js(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out, app_js="/*APPJS_MARKER*/")
    html = out.read_text(encoding="utf-8")
    assert "/*APPJS_MARKER*/" in html


def test_render_is_valid_parseable_html(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)

    class _P(HTMLParser):
        pass

    _P().feed(out.read_text(encoding="utf-8"))  # raises on malformed markup
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cockpit/test_render.py -v`
Expected: FAIL — `render_dashboard` undefined.

- [ ] **Step 3: Implement `render.py`**

```python
# src/weather_analytics/cockpit/render.py
"""Render the dataset into one self-contained dist/index.html via Jinja2."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from weather_analytics.cockpit import charts
from weather_analytics.cockpit.data import Dataset

_env = Environment(
    loader=PackageLoader("weather_analytics.cockpit", "templates"),
    autoescape=select_autoescape(),
)


def _safe(fn: Callable[[], object], default: object) -> object:
    """Render one chart defensively — a single bad chart can't abort the page."""
    try:
        return fn()
    except Exception:  # noqa: BLE001 - chart-level isolation is intentional
        return default


def _json_island(raw: dict) -> str:
    """Serialize the dataset for a <script> data island, neutralizing any
    "</..." so a stray "</script>" in a data field can't break out of the tag."""
    return json.dumps(raw, separators=(",", ":")).replace("</", "<\\/")


def render_dashboard(dataset: Dataset, out_path: Path, app_js: str = "") -> None:
    out_path = Path(out_path)
    context = {
        "manifest": dataset.manifest,
        "kpis": _safe(lambda: charts.fleet_kpis(dataset), []),
        "generation": _safe(lambda: charts.generation_series(dataset), None),
        "capacity_factor": _safe(lambda: charts.capacity_factor_series(dataset), None),
        "performance": _safe(lambda: charts.performance_series(dataset), None),
        "asset_bars": _safe(lambda: charts.asset_bars(dataset), []),
        "type_split": _safe(lambda: charts.type_split(dataset), []),
        "assets": dataset.assets,
        "data_island": _json_island(dataset.raw),
        "app_js": app_js,
    }
    html = _env.get_template("index.html.j2").render(**context)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
```

- [ ] **Step 4: Create the template**

Create the file below **verbatim** — it is the complete, final template. The two Jinja macros (`_line_svg`, `_hbars_html`) are defined at the top; leading whitespace before `<!doctype html>` is harmless. The UI card class is `.card` (NOT `.panel`) so the rendered HTML contains no `panel`/`bokeh`/`pyodide` substring — that is what the `test_render_writes_self_contained_html` guard checks.

```jinja
{# src/weather_analytics/cockpit/templates/index.html.j2 #}
{%- macro _line_svg(s) -%}
{%- if s -%}
<svg viewBox="0 0 {{ s.width }} {{ s.height }}" preserveAspectRatio="none" role="img">
  <path d="{{ s.area_path }}" fill="rgba(90,169,230,0.20)" />
  <polyline points="{{ s.polyline }}" fill="none" stroke="var(--accent)" stroke-width="2" />
</svg>
<div class="axis"><span>{{ s.x0_label }}</span><span>max {{ '%.2f'|format(s.y_max) }}</span><span>{{ s.x1_label }}</span></div>
{%- else -%}<div class="nodata">no data in range</div>{%- endif -%}
{%- endmacro -%}
{%- macro _hbars_html(rows, show_type) -%}
{%- if rows -%}
{%- for r in rows -%}
<div class="hbar-row"><div class="hbar-label">{{ r.label }}</div>
<div class="hbar-track"><div class="hbar-fill{% if show_type and r.asset_type == 'solar' %} solar{% endif %}" style="width: {{ r.pct }}%"></div></div>
<div class="hbar-value">{{ r.disp }}</div></div>
{%- endfor -%}
{%- else -%}<div class="nodata">no data in range</div>{%- endif -%}
{%- endmacro -%}
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Weather-Adjusted Generation Analytics</title>
<style>
  :root {
    --bg: #0f1115; --card: #171a21; --ink: #e6e9ef; --muted: #9aa4b2;
    --accent: #5aa9e6; --accent2: #7ee0c0; --line: #262b34;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink);
    font: 14px/1.5 -apple-system, Segoe UI, Roboto, sans-serif; }
  header { padding: 24px 20px 8px; }
  h1 { margin: 0; font-size: 20px; }
  .sub { color: var(--muted); font-size: 12px; margin-top: 4px; }
  .wrap { padding: 12px 20px 48px; max-width: 1000px; margin: 0 auto; }
  .controls { display: flex; flex-wrap: wrap; gap: 10px; margin: 12px 0; }
  .controls label { color: var(--muted); font-size: 12px; display: flex;
    flex-direction: column; gap: 4px; }
  .controls select, .controls input {
    background: var(--card); color: var(--ink); border: 1px solid var(--line);
    border-radius: 6px; padding: 6px 8px; }
  .kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 12px; margin: 12px 0; }
  .kpi { background: var(--card); border: 1px solid var(--line); border-radius: 10px;
    padding: 14px 16px; }
  .kpi .label { color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: .04em; }
  .kpi .value { font-size: 26px; font-weight: 600; margin-top: 6px; }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 10px;
    padding: 16px; margin: 12px 0; }
  .card h2 { margin: 0 0 10px; font-size: 13px; color: var(--muted);
    text-transform: uppercase; letter-spacing: .04em; }
  svg { width: 100%; height: auto; display: block; }
  .hbar-row { display: grid; grid-template-columns: 80px 1fr 90px; align-items: center;
    gap: 8px; margin: 6px 0; }
  .hbar-track { background: var(--line); border-radius: 5px; height: 12px; overflow: hidden; }
  .hbar-fill { background: var(--accent); height: 100%; }
  .hbar-fill.solar { background: var(--accent2); }
  .hbar-value { text-align: right; color: var(--muted); font-variant-numeric: tabular-nums; }
  .nodata { color: var(--muted); font-style: italic; }
  .axis { display: flex; justify-content: space-between; color: var(--muted);
    font-size: 11px; margin-top: 4px; }
</style>
</head>
<body>
<header>
  <h1>Weather-Adjusted Generation Analytics</h1>
  <div class="sub">
    {{ manifest.date_range_start }} → {{ manifest.date_range_end }} ·
    {{ manifest.asset_count }} assets · generated {{ manifest.generated_at }}
  </div>
</header>
<div class="wrap">
  <div class="controls">
    <label>asset type
      <select id="f-type">
        <option value="">all</option>
        <option value="wind">wind</option>
        <option value="solar">solar</option>
      </select>
    </label>
    <label>asset
      <select id="f-asset">
        <option value="">all</option>
        {% for a in assets %}<option value="{{ a.asset_id }}">{{ a.display_name }}</option>{% endfor %}
      </select>
    </label>
    <label>from<input type="date" id="f-start" value="{{ manifest.date_range_start }}" /></label>
    <label>to<input type="date" id="f-end" value="{{ manifest.date_range_end }}" /></label>
  </div>

  <div class="kpis" id="kpis">
    {% for k in kpis %}
    <div class="kpi"><div class="label">{{ k.label }}</div>
      <div class="value" data-kpi="{{ k.key }}">{{ k.value }}</div></div>
    {% endfor %}
  </div>

  <div class="card"><h2>net generation (MWh)</h2>
    <div id="chart-generation">{{ _line_svg(generation) }}</div></div>

  <div class="card"><h2>capacity factor</h2>
    <div id="chart-capacity_factor">{{ _line_svg(capacity_factor) }}</div></div>

  <div class="card"><h2>weather-adjusted performance score</h2>
    <div id="chart-performance">{{ _line_svg(performance) }}</div></div>

  <div class="card"><h2>capacity factor by asset</h2>
    <div id="chart-asset_bars">{{ _hbars_html(asset_bars, true) }}</div></div>

  <div class="card"><h2>generation by type</h2>
    <div id="chart-type_split">{{ _hbars_html(type_split, false) }}</div></div>
</div>

<script type="application/json" id="cockpit-data">{{ data_island | safe }}</script>
<script>{{ app_js | safe }}</script>
</body>
</html>
```

> Spec note: the spec's Views list says "per-asset table"; this plan renders that panel as horizontal bars (`_hbars_html(asset_bars, ...)`), consistent with afk-cockpit's hbar idiom and the client-side redraw in `app.js`. This is an intentional design choice, not drift — there is no `<table>` and no table CSS.

- [ ] **Step 5: Ensure templates/static ship with the package**

The package is under `src/`. Add a `static/` placeholder so the loader dir exists, and make hatch include non-`.py` files. In `pyproject.toml` under `[tool.hatch.build.targets.wheel]` (create if absent), ensure:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/weather_analytics"]
```

Hatch includes package data by default for listed packages, so `templates/*.j2` and `static/*.js` ship automatically. Create the static dir now so Task 5 has a home:

```bash
mkdir -p src/weather_analytics/cockpit/static
touch src/weather_analytics/cockpit/static/.gitkeep
```

- [ ] **Step 6: Run tests + ruff**

Run: `uv run pytest tests/cockpit/test_render.py -v && uv run ruff check src/weather_analytics/cockpit/render.py`
Expected: PASS (all 4 tests), no lint errors.

- [ ] **Step 7: Commit**

```bash
git add src/weather_analytics/cockpit/render.py src/weather_analytics/cockpit/templates src/weather_analytics/cockpit/static tests/cockpit/test_render.py pyproject.toml
git commit -m "feat(cockpit): jinja render to self-contained dist/index.html"
```

---

### Task 5: `static/app.js` — client-side filter + redraw

**Files:**
- Create: `src/weather_analytics/cockpit/static/app.js`
- Modify: `src/weather_analytics/cockpit/render.py` (default `app_js` to the bundled file's contents)
- Test: `tests/cockpit/test_render.py` (add an inline-wiring assertion); interactive behavior verified via the preview tools.

**Interfaces:**
- Consumes: the JSON island `#cockpit-data` (shape = `data.Dataset.raw`), the control ids `#f-type/#f-asset/#f-start/#f-end`, the KPI `[data-kpi]` spans, and the chart container ids `#chart-*`.
- Produces: no exports (IIFE). `render.py` now reads `static/app.js` and passes it as `app_js` by default.

- [ ] **Step 1: Write `app.js` (mirrors charts.py math)**

```javascript
/* src/weather_analytics/cockpit/static/app.js
   Light client-side interactivity. Reads the JSON island and, on filter change,
   recomputes KPIs and redraws the SVG line charts + hbars. The aggregation math
   mirrors weather_analytics.cockpit.charts (keep them in sync). */
(function () {
  "use strict";
  var el = document.getElementById("cockpit-data");
  if (!el) return;
  var D = JSON.parse(el.textContent);
  var assets = D.assets || [];
  var daily = D.daily || [];
  var weather = D.weather || [];
  var typeById = {};
  assets.forEach(function (a) { typeById[a.asset_id] = a.asset_type; });

  var fType = document.getElementById("f-type");
  var fAsset = document.getElementById("f-asset");
  var fStart = document.getElementById("f-start");
  var fEnd = document.getElementById("f-end");

  function num(v) { return v === null || v === undefined || isNaN(+v) ? 0 : +v; }

  function allowedIds() {
    var t = fType.value, a = fAsset.value;
    var ids = assets
      .filter(function (x) { return (!t || x.asset_type === t) && (!a || x.asset_id === a); })
      .map(function (x) { return x.asset_id; });
    return new Set(ids);
  }

  function inRange(d) {
    var s = fStart.value, e = fEnd.value;
    if (s && d < s) return false;
    if (e && d > e) return false;
    return true;
  }

  function filt(rows, ids) {
    return rows.filter(function (r) { return ids.has(r.asset_id) && inRange(r.date); });
  }

  function fmt(n, dp) { return n.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp }); }

  function setKpis(fd, fw) {
    var netGen = 0, curt = 0, cfSum = 0, perfSum = 0;
    fd.forEach(function (r) { netGen += num(r.total_net_generation_mwh); curt += num(r.total_curtailment_mwh); cfSum += num(r.daily_capacity_factor); });
    fw.forEach(function (r) { perfSum += num(r.performance_score); });
    var cf = fd.length ? cfSum / fd.length : 0;
    var perf = fw.length ? perfSum / fw.length : 0;
    put("capacity_factor", fmt(cf * 100, 1) + "%");
    put("net_generation", fmt(netGen, 0));
    put("performance_score", fmt(perf, 2));
    put("curtailment", fmt(curt, 0));
  }

  function put(key, val) {
    var n = document.querySelector('[data-kpi="' + key + '"]');
    if (n) n.textContent = val;
  }

  function byDateSum(rows, attr) {
    var acc = {};
    rows.forEach(function (r) { acc[r.date] = (acc[r.date] || 0) + num(r[attr]); });
    return Object.keys(acc).sort().map(function (d) { return [d, acc[d]]; });
  }

  function byDateMean(rows, attr) {
    var g = {};
    rows.forEach(function (r) { (g[r.date] = g[r.date] || []).push(num(r[attr])); });
    return Object.keys(g).sort().map(function (d) {
      var a = g[d]; return [d, a.reduce(function (s, v) { return s + v; }, 0) / a.length];
    });
  }

  function lineSvg(pairs, width, height, pad) {
    if (!pairs.length) return '<div class="nodata">no data in range</div>';
    var yMax = Math.max.apply(null, pairs.map(function (p) { return p[1]; })) || 1;
    var n = pairs.length;
    function x(i) { return pad + (n > 1 ? (i / (n - 1)) * (width - 2 * pad) : 0); }
    function y(v) { return height - pad - (v / yMax) * (height - 2 * pad); }
    var pts = pairs.map(function (p, i) { return [x(i), y(p[1])]; });
    var poly = pts.map(function (p) { return p[0].toFixed(1) + "," + p[1].toFixed(1); }).join(" ");
    var area = "M " + pts[0][0].toFixed(1) + " " + (height - pad).toFixed(1) + " " +
      pts.map(function (p) { return "L " + p[0].toFixed(1) + " " + p[1].toFixed(1); }).join(" ") +
      " L " + pts[n - 1][0].toFixed(1) + " " + (height - pad).toFixed(1) + " Z";
    return '<svg viewBox="0 0 ' + width + ' ' + height + '" preserveAspectRatio="none" role="img">' +
      '<path d="' + area + '" fill="rgba(90,169,230,0.20)" />' +
      '<polyline points="' + poly + '" fill="none" stroke="var(--accent)" stroke-width="2" /></svg>' +
      '<div class="axis"><span>' + pairs[0][0] + '</span><span>max ' + yMax.toFixed(2) +
      '</span><span>' + pairs[n - 1][0] + '</span></div>';
  }

  function hbars(rows, showType) {
    if (!rows.length) return '<div class="nodata">no data in range</div>';
    var maxV = Math.max.apply(null, rows.map(function (r) { return r.value; })) || 0;
    return rows.map(function (r) {
      var pct = maxV ? Math.max(1.5, (r.value / maxV) * 100) : 0;
      var cls = "hbar-fill" + (showType && r.type === "solar" ? " solar" : "");
      return '<div class="hbar-row"><div class="hbar-label">' + r.label + '</div>' +
        '<div class="hbar-track"><div class="' + cls + '" style="width: ' + pct + '%"></div></div>' +
        '<div class="hbar-value">' + fmt(r.value, 2) + '</div></div>';
    }).join("");
  }

  function setHtml(id, html) { var n = document.getElementById(id); if (n) n.innerHTML = html; }

  function assetBars(fd) {
    var g = {};
    fd.forEach(function (r) { (g[r.asset_id] = g[r.asset_id] || []).push(num(r.daily_capacity_factor)); });
    return Object.keys(g).sort().map(function (id) {
      var a = g[id]; return { label: id, value: a.reduce(function (s, v) { return s + v; }, 0) / a.length, type: typeById[id] || "" };
    });
  }

  function typeSplit(fd) {
    var t = {};
    fd.forEach(function (r) { var k = typeById[r.asset_id] || "unknown"; t[k] = (t[k] || 0) + num(r.total_net_generation_mwh); });
    return Object.keys(t).sort().map(function (k) { return { label: k, value: t[k] }; });
  }

  function apply() {
    var ids = allowedIds();
    var fd = filt(daily, ids), fw = filt(weather, ids);
    setKpis(fd, fw);
    setHtml("chart-generation", lineSvg(byDateSum(fd, "total_net_generation_mwh"), 720, 200, 8));
    setHtml("chart-capacity_factor", lineSvg(byDateMean(fd, "daily_capacity_factor"), 720, 160, 8));
    setHtml("chart-performance", lineSvg(byDateMean(fw, "performance_score"), 720, 160, 8));
    setHtml("chart-asset_bars", hbars(assetBars(fd), true));
    setHtml("chart-type_split", hbars(typeSplit(fd), false));
  }

  [fType, fAsset, fStart, fEnd].forEach(function (c) {
    if (c) c.addEventListener("change", apply);
  });
})();
```

- [ ] **Step 2: Default `render.py`'s `app_js` to the bundled file**

Edit `render.py`: add a module-level loader and use it as the default.

```python
# add near the top of render.py, after _env is defined
_STATIC_DIR = Path(__file__).parent / "static"


def _bundled_app_js() -> str:
    path = _STATIC_DIR / "app.js"
    return path.read_text(encoding="utf-8") if path.exists() else ""
```

Change the signature + body so an unset `app_js` loads the bundled script:

```python
def render_dashboard(dataset: Dataset, out_path: Path, app_js: str | None = None) -> None:
    if app_js is None:
        app_js = _bundled_app_js()
    out_path = Path(out_path)
    # ... rest unchanged (context uses app_js) ...
```

(The Task 4 test `test_render_inlines_app_js` passes `app_js="/*APPJS_MARKER*/"` explicitly, so it still works.)

- [ ] **Step 3: Add a wiring assertion to `test_render.py`**

```python
def test_render_defaults_to_bundled_app_js(dataset, tmp_path):
    out = tmp_path / "index.html"
    render_dashboard(dataset, out)  # no app_js -> bundled file
    html = out.read_text(encoding="utf-8")
    assert "cockpit-data" in html
    assert "addEventListener" in html  # app.js actually inlined
```

- [ ] **Step 4: Run tests + ruff**

Run: `uv run pytest tests/cockpit/test_render.py -v && uv run ruff check src/weather_analytics/cockpit/render.py`
Expected: PASS.

- [ ] **Step 5: Verify interactive behavior in the browser (preview tools)**

Build a dashboard from the fixtures and open it:

```bash
uv run python -m weather_analytics.cockpit build --export-dir tests/cockpit/fixtures --out dist/index.html
```

(Task 7 delivers the CLI; if running this step before Task 7, instead render via a one-off: `uv run python -c "from pathlib import Path; from weather_analytics.cockpit.data import load_dataset; from weather_analytics.cockpit.render import render_dashboard; render_dashboard(load_dataset(Path('tests/cockpit/fixtures')), Path('dist/index.html'))"`.)

Then use the preview tools: start a static server for `dist/`, load it, and confirm via `preview_snapshot` that the 4 KPIs render; drive `#f-type` to `solar` via `preview_fill`, and assert (via `preview_inspect`/`preview_snapshot`) that the `net_generation` KPI drops to the solar-only total (620) and the asset bars show only `S1`. Screenshot for the record. Fix `app.js` and re-verify if anything is off.

- [ ] **Step 6: Commit**

```bash
git add src/weather_analytics/cockpit/static/app.js src/weather_analytics/cockpit/render.py tests/cockpit/test_render.py
git commit -m "feat(cockpit): client-side filter and chart redraw"
```

---

### Task 6: `cloudflare.py` + `serve.py`

**Files:**
- Create: `src/weather_analytics/cockpit/cloudflare.py`
- Create: `src/weather_analytics/cockpit/serve.py`
- Test: `tests/cockpit/test_cloudflare.py`, `tests/cockpit/test_serve.py`

**Interfaces:**
- Produces:
  - `cloudflare.DEFAULT_PROJECT_NAME: str = "waga-dashboard"`; `cloudflare.Runner = Callable[[Sequence[str]], str]`; `cloudflare.deploy(dist_dir: Path, project_name: str = DEFAULT_PROJECT_NAME, branch: str = "main", runner: Runner = _default_runner) -> str`.
  - `serve.make_server(dist_dir: Path, port: int) -> ThreadingHTTPServer`; `serve.serve(dist_dir: Path, port: int = 8420) -> None`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/cockpit/test_cloudflare.py
from pathlib import Path

from weather_analytics.cockpit import cloudflare


def _recording_runner(calls):
    def run(argv):
        calls.append(list(argv))
        return "ok"
    return run


def test_deploy_invokes_npx_wrangler_with_project_and_branch():
    calls = []
    out = cloudflare.deploy(Path("/repo/dist"), runner=_recording_runner(calls))
    assert out == "ok"
    assert calls == [[
        "npx", "--yes", "wrangler", "pages", "deploy", "/repo/dist",
        "--project-name", "waga-dashboard", "--branch", "main", "--commit-dirty=true",
    ]]


def test_deploy_respects_overrides():
    calls = []
    cloudflare.deploy(Path("/d"), project_name="other", branch="dev", runner=_recording_runner(calls))
    assert "--project-name" in calls[0]
    assert calls[0][calls[0].index("--project-name") + 1] == "other"
    assert calls[0][calls[0].index("--branch") + 1] == "dev"
```

```python
# tests/cockpit/test_serve.py
import urllib.request
from pathlib import Path

from weather_analytics.cockpit.serve import make_server


def test_make_server_serves_index(tmp_path: Path):
    (tmp_path / "index.html").write_text("<h1>hi cockpit</h1>", encoding="utf-8")
    httpd = make_server(tmp_path, port=0)  # port 0 -> OS-assigned
    import threading
    t = threading.Thread(target=httpd.handle_request)
    t.start()
    try:
        port = httpd.server_address[1]
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/index.html", timeout=5).read()
        assert b"hi cockpit" in body
    finally:
        t.join(timeout=5)
        httpd.server_close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/cockpit/test_cloudflare.py tests/cockpit/test_serve.py -v`
Expected: FAIL — modules undefined.

- [ ] **Step 3: Implement `cloudflare.py`**

```python
# src/weather_analytics/cockpit/cloudflare.py
"""Deploy dist/ to Cloudflare Pages via `npx wrangler pages deploy`.

Copied from afk-cockpit. `npx` (not a bare `wrangler`) because launchd's minimal
PATH won't resolve a global install. `wrangler` reads CLOUDFLARE_API_TOKEN and
CLOUDFLARE_ACCOUNT_ID from the environment.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path

Runner = Callable[[Sequence[str]], str]

DEFAULT_PROJECT_NAME = "waga-dashboard"


def _default_runner(argv: Sequence[str]) -> str:
    return subprocess.run(list(argv), capture_output=True, text=True, check=True).stdout


def deploy(
    dist_dir: Path,
    project_name: str = DEFAULT_PROJECT_NAME,
    branch: str = "main",
    runner: Runner = _default_runner,
) -> str:
    """Upload dist_dir to Cloudflare Pages as a new deployment. Returns wrangler stdout."""
    return runner(
        [
            "npx",
            "--yes",
            "wrangler",
            "pages",
            "deploy",
            str(dist_dir),
            "--project-name",
            project_name,
            "--branch",
            branch,
            "--commit-dirty=true",
        ]
    )
```

- [ ] **Step 4: Implement `serve.py`**

```python
# src/weather_analytics/cockpit/serve.py
"""Minimal loopback static server for local preview of dist/."""

from __future__ import annotations

import functools
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def make_server(dist_dir: Path, port: int) -> ThreadingHTTPServer:
    """Build a ThreadingHTTPServer rooted at dist_dir, bound to loopback."""
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(dist_dir))
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


def serve(dist_dir: Path, port: int = 8420) -> None:
    """Serve dist_dir until interrupted."""
    httpd = make_server(Path(dist_dir), port)
    bound = httpd.server_address[1]
    print(f"serving {dist_dir} at http://127.0.0.1:{bound}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
```

- [ ] **Step 5: Run tests + ruff**

Run: `uv run pytest tests/cockpit/test_cloudflare.py tests/cockpit/test_serve.py -v && uv run ruff check src/weather_analytics/cockpit/cloudflare.py src/weather_analytics/cockpit/serve.py`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/weather_analytics/cockpit/cloudflare.py src/weather_analytics/cockpit/serve.py tests/cockpit/test_cloudflare.py tests/cockpit/test_serve.py
git commit -m "feat(cockpit): cloudflare deploy and local static serve"
```

---

### Task 7: `cli.py` + `__main__.py` — build/deploy/serve

**Files:**
- Create: `src/weather_analytics/cockpit/cli.py`
- Create: `src/weather_analytics/cockpit/__main__.py`
- Test: `tests/cockpit/test_cli.py`

**Interfaces:**
- Consumes: `data.load_dataset`, `render.render_dashboard`, `cloudflare.deploy`, `serve.serve`, `config.*`.
- Produces: `cli.main(argv: list[str] | None = None) -> int`. Subcommands: `build --export-dir <dir> --out <path>`; `deploy --dist <dir> --project-name <name> --branch <b>`; `serve --dist <dir> --port <n>`. `__main__.py`: `raise SystemExit(main())`.

- [ ] **Step 1: Write the failing test**

```python
# tests/cockpit/test_cli.py
from pathlib import Path

from weather_analytics.cockpit.cli import main

FIXTURES = Path(__file__).parent / "fixtures"


def test_build_writes_index(tmp_path):
    out = tmp_path / "dist" / "index.html"
    code = main(["build", "--export-dir", str(FIXTURES), "--out", str(out)])
    assert code == 0
    assert out.exists()
    html = out.read_text(encoding="utf-8")
    assert "weather-adjusted" in html.lower()
    assert "cockpit-data" in html


def test_deploy_calls_cloudflare(monkeypatch, tmp_path):
    seen = {}

    def fake_deploy(dist_dir, project_name="waga-dashboard", branch="main", runner=None):
        seen["dist"] = str(dist_dir)
        seen["project"] = project_name
        return "deployed"

    monkeypatch.setattr("weather_analytics.cockpit.cli.deploy", fake_deploy)
    code = main(["deploy", "--dist", str(tmp_path)])
    assert code == 0
    assert seen["dist"] == str(tmp_path)
    assert seen["project"] == "waga-dashboard"


def test_unknown_command_errors():
    import pytest
    with pytest.raises(SystemExit):
        main(["frobnicate"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cockpit/test_cli.py -v`
Expected: FAIL — `cli.main` undefined.

- [ ] **Step 3: Implement `cli.py`**

```python
# src/weather_analytics/cockpit/cli.py
"""`python -m weather_analytics.cockpit build|deploy|serve`."""

from __future__ import annotations

import argparse
from pathlib import Path

from weather_analytics.cockpit import config
from weather_analytics.cockpit.cloudflare import DEFAULT_PROJECT_NAME, deploy
from weather_analytics.cockpit.data import load_dataset
from weather_analytics.cockpit.render import render_dashboard
from weather_analytics.cockpit.serve import serve


def _build(args: argparse.Namespace) -> int:
    dataset = load_dataset(Path(args.export_dir))
    out = Path(args.out)
    render_dashboard(dataset, out)
    print(f"built {out} from {args.export_dir}")
    return 0


def _deploy(args: argparse.Namespace) -> int:
    out = deploy(Path(args.dist), project_name=args.project_name, branch=args.branch)
    print(out.strip() or f"deployed {args.dist} to {config.SITE_URL}")
    return 0


def _serve(args: argparse.Namespace) -> int:
    serve(Path(args.dist), port=args.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="weather_analytics.cockpit")
    sub = parser.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="render the static dashboard")
    b.add_argument("--export-dir", default=config.DEFAULT_EXPORT_DIR)
    b.add_argument("--out", default=config.DEFAULT_OUT)
    b.set_defaults(func=_build)

    d = sub.add_parser("deploy", help="deploy dist/ to Cloudflare Pages")
    d.add_argument("--dist", default=config.DEFAULT_DIST_DIR)
    d.add_argument("--project-name", default=DEFAULT_PROJECT_NAME)
    d.add_argument("--branch", default="main")
    d.set_defaults(func=_deploy)

    s = sub.add_parser("serve", help="serve dist/ locally")
    s.add_argument("--dist", default=config.DEFAULT_DIST_DIR)
    s.add_argument("--port", type=int, default=8420)
    s.set_defaults(func=_serve)

    args = parser.parse_args(argv)
    return args.func(args)
```

- [ ] **Step 4: Implement `__main__.py`**

```python
# src/weather_analytics/cockpit/__main__.py
from weather_analytics.cockpit.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run tests + ruff + smoke the module entry**

Run:
```bash
uv run pytest tests/cockpit/test_cli.py -v
uv run ruff check src/weather_analytics/cockpit
uv run python -m weather_analytics.cockpit build --export-dir tests/cockpit/fixtures --out dist/index.html && test -f dist/index.html && echo OK
```
Expected: tests PASS, no lint errors, `OK` printed.

- [ ] **Step 6: Run the full package suite**

Run: `uv run pytest tests/cockpit/ -v`
Expected: all tasks' tests PASS together.

- [ ] **Step 7: Commit**

```bash
git add src/weather_analytics/cockpit/cli.py src/weather_analytics/cockpit/__main__.py tests/cockpit/test_cli.py
git commit -m "feat(cockpit): build/deploy/serve CLI"
```

---

### Task 8: Remove the old Pyodide dashboard + wire the daily chain

> **Branch prerequisite (see Global Constraints):** this task edits `scripts/run_scheduled.py`, which exists only on `feat/local-launchd-scheduling`. Confirm your branch has that file before starting.

**Files:**
- Delete: `src/weather_analytics/dashboard/` (whole directory)
- Delete: `scripts/build_dashboard_app.py`, `scripts/push_dashboard_build.py`, `.github/workflows/build-dashboard.yml`
- Delete: `src/weather_analytics/checks/dashboard.py` (the `waga_dashboard_export_commit_landed` asset check — depends on the deleted publish asset + resource)
- Delete: `src/weather_analytics/resources/portfolio_repo.py` (defines `PortfolioRepoResource`, now unused)
- Modify: `src/weather_analytics/assets/analytics/dashboard_export.py` (remove the `waga_dashboard_export_publish` asset; keep `waga_dashboard_export_build`)
- Modify: `src/weather_analytics/assets/analytics/__init__.py` (drop `waga_dashboard_export_publish` import + `__all__` entry)
- Modify: `src/weather_analytics/checks/__init__.py` (drop `waga_dashboard_export_commit_landed` import + `__all__` entry)
- Modify: `src/weather_analytics/resources/__init__.py` (drop `PortfolioRepoResource` import + `__all__` entry)
- Modify: `src/weather_analytics/schedules.py` (delete `waga_daily_dashboard_schedule`)
- Modify: `src/weather_analytics/definitions.py` (the `Definitions(...)` object — remove all four removed symbols from imports, `assets=`, `asset_checks=`, `schedules=`, and `resources=`)
- Modify: `pyproject.toml` (drop the `dashboard` extra; optionally drop panel/bokeh from `dev`; delete the stale `dashboard/*` per-file-ignore)
- Modify: `.env.example` (add CF keys, remove dead portfolio keys)
- Modify: `scripts/run_scheduled.py` (re-enable export build; add post-Dagster cockpit steps)
- Modify/Delete: tests importing the removed symbols (see Step 9)

**Interfaces:** none exported; this is removal + orchestration wiring. The deliverable is a repo that passes `uv run dagster definitions validate`, `uv run pytest`, and `uv run ruff check`.

> **Grounding (verified on `feat/local-launchd-scheduling`):** the four removed symbols are wired as follows — `definitions.py` imports `waga_dashboard_export_publish` (from `assets.analytics`), `waga_dashboard_export_commit_landed` (from `checks`), `PortfolioRepoResource` (from `resources.portfolio_repo`), and `waga_daily_dashboard_schedule` (from `schedules`), and registers them in `assets=`, `asset_checks=`, `resources={"portfolio_repo": ...}`, and `schedules=` respectively. The check `waga_dashboard_export_commit_landed` (`checks/dashboard.py`) is `@asset_check(asset=AssetKey(["waga_dashboard_export_publish"]))` and takes a `PortfolioRepoResource` param — so it MUST be removed or `dagster definitions validate` fails. Both `assets/analytics/__init__.py` and `resources/__init__.py` re-export removed symbols in their `__all__`.

- [ ] **Step 1: Delete the old dashboard package + scripts + workflow**

```bash
git rm -r src/weather_analytics/dashboard
git rm scripts/build_dashboard_app.py scripts/push_dashboard_build.py .github/workflows/build-dashboard.yml
```

- [ ] **Step 2: Remove the publish asset, the asset check, and the resource (+ their package exports)**

Do all of the following so nothing dangles:

1. `src/weather_analytics/assets/analytics/dashboard_export.py` — delete the `waga_dashboard_export_publish` asset function (keep `waga_dashboard_export_build`). Remove now-unused imports it required (e.g. the GitHub client, base64) if nothing else in the file uses them.
2. `src/weather_analytics/assets/analytics/__init__.py` — remove `waga_dashboard_export_publish` from both the `from ...dashboard_export import (...)` block and `__all__`.
3. `git rm src/weather_analytics/checks/dashboard.py` — deletes the `waga_dashboard_export_commit_landed` check (it targets the deleted publish asset and needs `PortfolioRepoResource`).
4. `src/weather_analytics/checks/__init__.py` — remove the `from weather_analytics.checks.dashboard import waga_dashboard_export_commit_landed` import and the `"waga_dashboard_export_commit_landed"` entry in `__all__`.
5. `git rm src/weather_analytics/resources/portfolio_repo.py` — deletes `PortfolioRepoResource`.
6. `src/weather_analytics/resources/__init__.py` — remove the `from ...portfolio_repo import PortfolioRepoResource` import and `PortfolioRepoResource` from `__all__` (leaves `DltIngestionResource`, `WAGASnowflakeResource`).

Then confirm nothing still references the removed symbols:

```bash
grep -rn "waga_dashboard_export_publish\|PortfolioRepoResource\|waga_dashboard_export_commit_landed\|portfolio_repo" src/ && echo "STILL REFERENCED — fix each hit" || echo "clean"
```

- [ ] **Step 3: Remove the publish schedule + unwire the Definitions object**

In `src/weather_analytics/schedules.py`, delete the `waga_daily_dashboard_schedule` definition.

In `src/weather_analytics/definitions.py` (this file holds the `Definitions(...)` object), remove **all four** removed symbols:
- the imports: `waga_dashboard_export_publish` (from `weather_analytics.assets.analytics`), `waga_dashboard_export_commit_landed` (from `weather_analytics.checks`), `PortfolioRepoResource` (from `weather_analytics.resources.portfolio_repo`), and `waga_daily_dashboard_schedule` (from `weather_analytics.schedules`);
- `waga_dashboard_export_publish` from the `assets=[...]` list (keep `waga_dashboard_export_build`);
- `waga_dashboard_export_commit_landed` from the `asset_checks=[...]` list;
- `waga_daily_dashboard_schedule` from the `schedules=[...]` list;
- the `"portfolio_repo": PortfolioRepoResource(...)` entry from the `resources={...}` mapping.

- [ ] **Step 4: Validate Dagster definitions load**

Run: `uv run dagster definitions validate`
Expected: PASS (no reference to the removed asset/check/resource/schedule). If it fails, it will name the dangling reference — grep `src/` for that symbol and remove the straggler.

- [ ] **Step 5: Drop the dashboard deps + stale lint ignore + document env keys**

In `pyproject.toml`:
- Delete the `dashboard = ["panel>=1.6.0,<1.7.0", "bokeh>=3.5.0,<3.8.0"]` entry under `[project.optional-dependencies]` (and the group if now empty).
- Remove the `"panel>=1.6.0,<1.7.0"` and `"bokeh>=3.5.0,<3.8.0"` lines from the `dev` extra too (they were only there so the deleted dashboard's tests could import panel/bokeh; nothing imports them now).
- Delete the stale `"src/weather_analytics/dashboard/*" = [...]` block under `[tool.ruff.per-file-ignores]` (it points at the deleted directory). Also remove the `panel.*` / `bokeh.*` entries from the `[[tool.mypy.overrides]]` `module` list if present (dead once the dashboard is gone).

In `.env.example`: remove the `WAGA_PORTFOLIO_REPO_OWNER/NAME/BRANCH/TOKEN` lines; add:

```bash
# Cloudflare Pages (static dashboard deploy). wrangler reads these from the env.
CLOUDFLARE_API_TOKEN=
CLOUDFLARE_ACCOUNT_ID=
```

- [ ] **Step 6: Re-enable the export build in the daily chain**

In `scripts/run_scheduled.py`, update the `JOBS["daily"]` list: append a third Dagster step after the `group:default` step, and update the NOTE comment (it currently says the dashboard assets are excluded). Add:

```python
        [
            "asset",
            "materialize",
            "--select",
            "waga_dashboard_export_build",
            "-m",
            MODULE,
        ],
```

Replace the stale NOTE block above `JOBS` with:

```python
# NOTE: the daily chain ends by (1) materializing waga_dashboard_export_build,
# which writes the 4 JSON exports to dashboard_exports/, then (2) running the
# cockpit build + deploy POST_STEPS below to render and publish the static
# dashboard to Cloudflare Pages. The old waga_dashboard_export_publish asset
# (push to a stale portfolio 'master') was removed with the Pyodide dashboard.
```

- [ ] **Step 7: Add the post-Dagster cockpit steps**

The runner wraps every `JOBS` step as `uv run python -m dagster ...`, so the cockpit commands (not Dagster) go in a separate `POST_STEPS` map of full argv lists, run after the Dagster chain succeeds. Add near `JOBS`:

```python
# Post-Dagster steps: full argv lists run verbatim (NOT wrapped in dagster).
# For `daily`, render the static dashboard from the fresh JSON exports and
# deploy it to Cloudflare Pages. wrangler reads CLOUDFLARE_* from the env that
# load_dotenv() populated above.
POST_STEPS: dict[str, list[list[str]]] = {
    "daily": [
        [UV, "run", "python", "-m", "weather_analytics.cockpit", "build"],
        [UV, "run", "python", "-m", "weather_analytics.cockpit", "deploy"],
    ],
}
```

Then, in `main()`, after the Dagster `for index, step ...` loop completes successfully (after `emit("=== all steps succeeded ===")` — but before `return 0`), run the post-steps with the same logging/abort contract:

```python
        for index, cmd in enumerate(POST_STEPS.get(args.job, []), start=1):
            emit(f"--- post-step {index}/{len(POST_STEPS.get(args.job, []))}: {' '.join(cmd)} ---")
            result = subprocess.run(  # noqa: PLW1510 - returncode handled below
                cmd, cwd=REPO, stdout=log, stderr=subprocess.STDOUT
            )
            log.flush()
            if result.returncode != 0:
                emit(f"POST-STEP FAILED (exit {result.returncode}) on step {index}")
                return result.returncode
        emit("=== all post-steps succeeded ===")
        return 0
```

Remove the old `emit("=== all steps succeeded ===")` / `return 0` that ended the `with` block so control flows into the post-steps (i.e., keep a single success/return path at the end).

- [ ] **Step 8: Validate the runner still parses + dry-check the daily chain shape**

Run:
```bash
uv run python -c "import ast; ast.parse(open('scripts/run_scheduled.py').read()); print('parse OK')"
uv run ruff check scripts/run_scheduled.py
uv run python -c "import importlib.util, pathlib; spec=importlib.util.spec_from_file_location('rs', pathlib.Path('scripts/run_scheduled.py')); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m); print('daily steps:', len(m.JOBS['daily']), 'post:', len(m.POST_STEPS['daily']))"
```
Expected: `parse OK`, no lint errors, `daily steps: 3 post: 2`.

- [ ] **Step 9: Clean up the tests that import removed symbols, then full repo test + lint**

First find every test that imports something we deleted (these fail at collection, not just assertion):

```bash
grep -rln "waga_dashboard_export_publish\|waga_dashboard_export_commit_landed\|waga_daily_dashboard_schedule\|PortfolioRepoResource\|weather_analytics.dashboard\|build_dashboard_app\|push_dashboard_build" tests/
```

Expected hits and the exact action for each (verified on `feat/local-launchd-scheduling`):
- `tests/unit/test_dashboard_checks.py` — imports `waga_dashboard_export_commit_landed`. **Delete the file** (`git rm`); the check no longer exists.
- `tests/unit/test_schedules.py` — imports and asserts on `waga_daily_dashboard_schedule`. **Edit:** drop that name from the import and delete only its schedule test cases; keep the ingestion/dbt/weekly schedule tests.
- `tests/unit/test_dashboard_export.py` — imports **both** `waga_dashboard_export_build` (keep) and `waga_dashboard_export_publish` (removed). **Edit:** drop `waga_dashboard_export_publish` from the import and delete the `test_publish_*` functions; keep the `test_build_*` tests.
- The Panel/Pyodide suite — anything under `tests/unit/dashboard/`, plus `test_dashboard_data_loader.py`, `test_dashboard_theme.py`, and `test_dashboard_bundler.py` (the last imports `scripts.build_dashboard_app`, deleted in Step 1). **Delete these files** (`git rm`).

Re-run the grep until it returns nothing. Then:

Run: `uv run pytest && uv run ruff check .`
Expected: PASS. If pytest errors at **collection** (not assertion), the grep missed an importer — find it in the traceback and apply the same delete-or-edit rule.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "chore(dashboard): remove Pyodide dashboard; wire cockpit into daily chain

Delete src/weather_analytics/dashboard, the publish asset + PortfolioRepoResource,
waga_daily_dashboard_schedule, the build/push scripts and build-dashboard.yml, and
the panel/bokeh extra. Re-enable waga_dashboard_export_build in the daily launchd
chain and add cockpit build+deploy post-steps targeting Cloudflare Pages."
```

---

### Task 9: Repoint the portfolio card (separate repo)

**Files:**
- Modify: `/Users/cdcoonce/Developer/GitHub/PortfolioWebsite/src/data/portfolio.js:112`

**Interfaces:** none. The WAGA card keeps its cockpit-SVG hero (`slug: 'waga'`); only the outbound link changes from the GitHub repo to the live dashboard.

- [ ] **Step 1: Repoint the href**

In `PortfolioWebsite/src/data/portfolio.js`, in the `'Weather-Adjusted Generation Analytics'` entry, change:

```js
    href: 'https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics',
```
to:
```js
    href: 'https://waga-dashboard.pages.dev',
```

- [ ] **Step 2: Run the portfolio CI gate**

```bash
cd /Users/cdcoonce/Developer/GitHub/PortfolioWebsite
npm test && npm run lint && npm run build
```
Expected: PASS (full local CI gate before any push).

- [ ] **Step 3: Commit**

```bash
cd /Users/cdcoonce/Developer/GitHub/PortfolioWebsite
git add src/data/portfolio.js
git commit -m "feat(work): point WAGA card to live dashboard"
```

> The portfolio card should only be repointed once the Cloudflare project is live and a first deploy has landed (so the link resolves). Sequence Task 9 after the first successful `cockpit deploy`.

---

## Self-Review

**1. Spec coverage:**
- Standalone module + CLI (spec Decisions #4) → Tasks 1–7. ✓
- afk-cockpit data→charts→render pattern, inline SVG, no chart lib (Decisions #3) → Tasks 3–4. ✓
- Static + light interactivity (Decisions #2) → Task 5 (`app.js` + JSON island). ✓
- Own Cloudflare Pages via `npx wrangler`, `waga-dashboard` (Decisions #1, #5) → Task 6 + pre-exec checklist. ✓
- Keep `waga_dashboard_export_build`; remove `_publish` asset + its `PortfolioRepoResource` + the `waga_dashboard_export_commit_landed` **asset check** + all four `__init__`/`Definitions` re-exports (spec Removals) → Task 8 Steps 2–4. ✓
- Remove `waga_daily_dashboard_schedule`; retire scripts + `build-dashboard.yml`; drop `panel/bokeh` from both the `dashboard` and `dev` extras; delete the stale `dashboard/*` lint ignore; CF env keys; dead portfolio keys (Removals) → Task 8 Steps 1,3,5. ✓
- Orchestration: build step re-enabled + cockpit build/deploy post-steps in daily chain (spec Orchestration) → Task 8 Steps 6–7. ✓
- Test cleanup for removed symbols (not just Panel/Pyodide) → Task 8 Step 9 (explicit file-by-file). ✓
- Repoint portfolio card href (Removals) → Task 9. ✓
- Branch prerequisite (spec Prerequisite) → Global Constraints + Task 8 banner. ✓
- Testing: data/charts/render/cloudflare/serve/cli pytest + preview for JS (spec Testing) → each task's tests + Task 5 Step 5. ✓

**2. Placeholder scan:** No TBD/TODO. CF URL is concrete (`https://waga-dashboard.pages.dev`). Task 8 names `definitions.py` as the `Definitions(...)` home explicitly (grounded on-branch) with exact symbols to remove — no "wherever" hand-waving.

**3. Type consistency:** `Dataset`/`Asset`/`DailyRow`/`WeatherRow`/`Manifest` defined in Task 2 and consumed with the same field names in Tasks 3–4, 7. `charts.fleet_kpis` returns `{"key","label","value"}` — consumed by the template (`data-kpi="{{ k.key }}"`) and mirrored by `app.js` `put(key,...)`. `line_series` dict keys (`area_path`, `polyline`, `y_max`, `x0_label`, `x1_label`) match the template macro `_line_svg` and `app.js` `lineSvg` output. `cloudflare.deploy` signature identical in Task 6 impl and Task 7 CLI call and Task 8 POST_STEPS invocation (`python -m weather_analytics.cockpit deploy`). ✓

**4. Adversarial-verification fixes (2026-07-04):** a 5-critic pass (grounded by running the plan's code) found and this revision fixed: (a) the `test_render` `"panel"` banned-word guard colliding with the template's own `.panel` class → CSS/class renamed to `.card`; (b) Task 8 not removing the `waga_dashboard_export_commit_landed` asset check + `portfolio_repo` resource file + the `assets.analytics`/`resources`/`checks` `__init__` re-exports → `definitions validate` would fail → now explicit; (c) vague test cleanup → explicit per-file edits; (d) the multi-pass template with a leftover `_line.svg.j2` include → replaced with one complete listing; (e) ruff `T201`/`ANN001` failing the lint gate → cockpit `T201` per-file-ignore + full parameter annotations; (f) JSON-island `</script>` breakout → hardened via `_json_island`; (g) Task 8/9 mislabel in the branch-prereq bullet. Minor "per-asset table → bars" is a noted intentional divergence.
