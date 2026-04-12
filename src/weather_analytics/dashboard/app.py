"""WAGA dashboard Panel app — Phase 1 tracer bullet.

Renders a minimal single-chart dashboard that proves the end-to-end
pipeline (Dagster -> JSON -> portfolio repo -> Pyodide -> Panel -> Bokeh)
works in a real browser. Later dev-cycle phases expand this into the
full three-tab dashboard with filter bar, KPI row, and reactive components.

**Self-contained on purpose.**
``panel convert --to pyodide-worker`` runs this file as a standalone
script inside Pyodide, not as part of the ``weather_analytics`` package.
That means it cannot do ``from weather_analytics.dashboard.data_loader
import ...`` — the package isn't on Pyodide's ``sys.path``. To keep Phase
1 building cleanly, this module inlines the small pieces it needs
(palette, theme JSON, async data fetcher) and intentionally does **not**
import from sibling modules in ``weather_analytics.dashboard``.

``theme.py`` and ``data_loader.py`` still exist and are covered by unit
tests because they remain the source of truth for Phase 2+ component
code. When Phase 2 adds real components and the duplication becomes
meaningful, switch to one of:

1. ``panel convert --requirements theme.py data_loader.py app.py`` —
   declare the local modules so the worker can import them.
2. Build-step concatenation that inlines helpers into a bundled
   ``app_bundled.py`` before running ``panel convert``.
3. Publish ``weather_analytics.dashboard`` as a wheel and install via
   ``micropip`` inside the worker.

Do not reintroduce ``from weather_analytics.dashboard.* import ...``
without also picking one of the strategies above — the current Panel
tooling will fail the build with ``ModuleNotFoundError``.

Notes
-----
- Panel and Bokeh are imported at module top so ``panel convert`` can
  detect the top-level ``.servable()`` call without indirection.
- Data is fetched asynchronously via ``pyodide.http.pyfetch`` in the
  browser, falling back to ``urllib`` for local ``panel serve`` runs.
- Fetch failures render a banner instead of crashing.
- Schema version mismatches render a non-blocking warning banner.
- ``console.error`` is emitted for every browser-visible error so
  operators can see them in the browser DevTools console.
"""

import json
import sys
from typing import Any

import panel as pn
from bokeh.io import curdoc
from bokeh.plotting import figure
from bokeh.themes import Theme

_DASHBOARD_TITLE = "Weather-Adjusted Generation Analytics"
_DASHBOARD_SUBTITLE = (
    "Renewable asset performance with weather-adjusted correlations — "
    "Phase 1 tracer bullet."
)
_EXPECTED_SCHEMA_VERSION = "1.0"
_DAILY_PERFORMANCE_PATH = "./data/daily_performance.json"
_MANIFEST_PATH = "./data/manifest.json"
_HTTP_ERROR_FLOOR = 400

# Keep in sync with ``weather_analytics.dashboard.theme.DATA_PALETTE``.
_DATA_PRIMARY = "#353535"

# Keep in sync with ``weather_analytics.dashboard.theme.build_theme_json``.
_THEME_JSON: dict[str, Any] = {
    "attrs": {
        "figure": {
            "background_fill_color": "#ffffff",
            "border_fill_color": "#ffffff",
            "outline_line_color": None,
        },
        "Axis": {
            "axis_label_text_font": "Poppins",
            "axis_label_text_font_size": "12px",
            "axis_label_text_color": "#353535",
            "axis_label_text_font_style": "normal",
            "major_label_text_font": "Poppins",
            "major_label_text_color": "#555555",
            "major_tick_line_color": "#353535",
            "minor_tick_line_color": None,
            "axis_line_color": "#353535",
        },
        "Grid": {
            "grid_line_color": "#f0f0f0",
            "grid_line_dash": [4, 4],
        },
        "Title": {
            "text_font": "Poppins",
            "text_font_size": "14px",
            "text_font_style": "normal",
            "text_color": "#353535",
        },
        "Legend": {
            "background_fill_alpha": 0.9,
            "border_line_color": None,
            "label_text_font": "Poppins",
            "label_text_color": "#555555",
        },
        "Toolbar": {
            "logo": None,
        },
    }
}


def _console_error(message: str) -> None:
    """Emit ``console.error`` in the browser (no-op outside Pyodide)."""
    if "pyodide" in sys.modules:
        try:
            from js import console

            console.error(message)
        except ImportError:
            pass


async def _fetch_text(url: str) -> str:
    """Fetch a URL and return response body as text.

    Uses ``pyodide.http.pyfetch`` in the browser and ``urllib`` locally.
    ``requests`` is deliberately not used because its socket backend is
    unavailable in the Pyodide sandbox.
    """
    if "pyodide" in sys.modules:
        from pyodide.http import pyfetch

        response = await pyfetch(url)
        if response.status >= _HTTP_ERROR_FLOOR:
            msg = f"Fetch of {url} failed with HTTP {response.status}"
            raise RuntimeError(msg)
        return await response.string()

    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(url) as response:
            return str(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        msg = f"Fetch of {url} failed: {exc}"
        raise RuntimeError(msg) from exc


async def _load_json(url: str) -> Any:
    """Fetch and parse a JSON URL."""
    body = await _fetch_text(url)
    return json.loads(body)


def _error_banner(message: str) -> pn.pane.Alert:
    """Return a Panel pane rendering a warning banner."""
    return pn.pane.Alert(message, alert_type="warning")


def _schema_mismatch_banner(actual_version: str) -> pn.pane.Alert:
    """Return a banner for schema version mismatches (non-blocking)."""
    message = (
        f"Data schema version `{actual_version}` does not match "
        f"expected `{_EXPECTED_SCHEMA_VERSION}`. The display may "
        f"be incorrect."
    )
    return pn.pane.Alert(message, alert_type="warning")


def _render_tracer_chart(rows: list[dict[str, Any]]) -> Any:
    """Render the Phase 1 tracer chart: fleet total generation over time."""
    if not rows:
        return pn.pane.Markdown("_No data available for the current range._")

    totals: dict[str, float] = {}
    for row in rows:
        date_key = str(row.get("date", ""))
        if not date_key:
            continue
        val = row.get("total_net_generation_mwh")
        if val is None:
            continue
        totals[date_key] = totals.get(date_key, 0.0) + float(val)

    if not totals:
        return pn.pane.Markdown("_No generation values found in payload._")

    sorted_dates = sorted(totals.keys())
    sorted_values = [totals[d] for d in sorted_dates]

    fig = figure(
        title="Fleet Total Net Generation — Daily",
        x_axis_label="Date",
        y_axis_label="MWh",
        sizing_mode="stretch_width",
        height=360,
        tools="",
        toolbar_location=None,
    )
    fig.line(
        x=sorted_dates,
        y=sorted_values,
        line_color=_DATA_PRIMARY,
        line_width=2,
    )
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


async def build_body() -> pn.Column:
    """Fetch data and build the dashboard body (banners + chart)."""
    banners: list[Any] = []

    try:
        manifest_raw = await _load_json(_MANIFEST_PATH)
    except Exception as exc:
        _console_error(f"Failed to load manifest: {exc}")
        return pn.Column(
            _error_banner(
                "Data temporarily unavailable. Last successful refresh: "
                "never. Check back shortly."
            ),
            sizing_mode="stretch_width",
        )

    actual_version = str(manifest_raw.get("schema_version", ""))
    if actual_version != _EXPECTED_SCHEMA_VERSION:
        _console_error(
            f"Schema mismatch: got {actual_version}, "
            f"expected {_EXPECTED_SCHEMA_VERSION}"
        )
        banners.append(_schema_mismatch_banner(actual_version))

    try:
        daily_raw = await _load_json(_DAILY_PERFORMANCE_PATH)
    except Exception as exc:
        _console_error(f"Failed to load daily_performance.json: {exc}")
        return pn.Column(
            _error_banner(
                "Data temporarily unavailable. Refresh failed while "
                "loading daily performance metrics."
            ),
            sizing_mode="stretch_width",
        )

    if not isinstance(daily_raw, list):
        _console_error("daily_performance.json is not a JSON array")
        return pn.Column(
            _error_banner(
                "Data format error: daily_performance.json is malformed."
            ),
            sizing_mode="stretch_width",
        )

    chart = _render_tracer_chart(daily_raw)
    return pn.Column(*banners, chart, sizing_mode="stretch_width")


# ---------------------------------------------------------------------------
# Top-level Panel app assembly
#
# ``panel convert`` and ``panel serve`` both execute this file as a script
# and look for a top-level ``.servable()`` call. That call must happen at
# module top level — putting it inside a function that no one calls will
# leave the Bokeh document empty and fail the build with
# "file does not publish any Panel contents".
# ---------------------------------------------------------------------------

pn.extension(sizing_mode="stretch_width")
# Apply the Bokeh theme to the current document so every figure in this
# app picks it up. ``pn.config.theme`` is for Panel's design system
# (material/default/dark) and does NOT accept a Bokeh ``Theme`` object.
curdoc().theme = Theme(json=_THEME_JSON)

_header = pn.pane.Markdown(f"# {_DASHBOARD_TITLE}\n\n{_DASHBOARD_SUBTITLE}")
pn.Column(_header, build_body, sizing_mode="stretch_width").servable()
