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

_STATIC_DIR = Path(__file__).parent / "static"


def _bundled_app_js() -> str:
    path = _STATIC_DIR / "app.js"
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _safe(fn: Callable[[], object], default: object) -> object:
    """Render one chart defensively — a single bad chart can't abort the page."""
    try:
        return fn()
    except Exception:  # chart-level isolation is intentional
        return default


def _json_island(raw: dict) -> str:
    """Serialize the dataset for a <script> data island, neutralizing any
    "</..." so a stray "</script>" in a data field can't break out of the tag."""
    return json.dumps(raw, separators=(",", ":")).replace("</", "<\\/")


def render_dashboard(
    dataset: Dataset, out_path: Path, app_js: str | None = None
) -> None:
    if app_js is None:
        app_js = _bundled_app_js()
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
