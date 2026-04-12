"""WAGA dashboard Panel app — Phase 2.

Renders a minimal single-chart dashboard that proves the end-to-end
pipeline (Dagster -> JSON -> portfolio repo -> Pyodide -> Panel -> Bokeh)
works in a real browser. Later dev-cycle phases expand this into the
full three-tab dashboard with filter bar, KPI row, and reactive components.

**Bundler-aware imports.**
``panel convert --to pyodide-worker`` runs this file as a standalone
script inside Pyodide, not as part of the ``weather_analytics`` package.
The build script (``scripts/build_dashboard_app.py``) concatenates
``theme.py`` and ``data_loader.py`` ahead of this file and strips the
``from weather_analytics.dashboard.*`` import lines, so the symbols
are already in scope when Pyodide executes the bundle.

Do not remove these imports without also updating ``build_dashboard_app.py``
and the module list in ``MODULES_TO_INLINE``.

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

import sys
from datetime import datetime
from typing import Any

import panel as pn
from bokeh.io import curdoc
from bokeh.themes import Theme

from weather_analytics.dashboard.data_loader import (
    EXPECTED_SCHEMA_VERSION,
    load_daily_performance,
    load_manifest,
)
from weather_analytics.dashboard.theme import build_theme_json

_DASHBOARD_TITLE = "Weather-Adjusted Generation Analytics"
_DASHBOARD_SUBTITLE = (
    "Renewable asset performance with weather-adjusted correlations — Phase 2."
)
_DAILY_PERFORMANCE_PATH = "./data/daily_performance.json"
_MANIFEST_PATH = "./data/manifest.json"

# Keep in sync with ``weather_analytics.dashboard.theme.DATA_PALETTE``.
_DATA_PRIMARY = "#353535"

# Inlined portfolio CSS so ``panel convert`` produces a self-contained
# bundle. Keep this in sync with
# ``weather_analytics.dashboard.static.portfolio.css``; the full CSS file
# remains the source of truth for Phase 3+ components and for local
# development via ``panel serve``.
_PORTFOLIO_CSS = """
@import url("https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&display=swap");

:root {
  --color-text-primary: #353535;
  --color-text-secondary: rgb(85, 85, 85);
  --color-bg-white: #fff;
  --color-bg-light: #f9f9f9;
  --color-border: rgb(53, 53, 53);
  --color-shadow: rgba(0, 0, 0, 0.1);
}

body,
.bk-root,
.bk,
.pn-material {
  font-family: "Poppins", -apple-system, BlinkMacSystemFont, sans-serif !important;
  color: var(--color-text-primary);
  background: var(--color-bg-white);
}

h1, h2, h3, h4 {
  font-family: "Poppins", sans-serif !important;
  font-weight: 600;
  color: var(--color-text-primary);
  letter-spacing: -0.01em;
}

h1 {
  font-size: 1.75rem;
  margin-top: 0;
}

p {
  color: var(--color-text-secondary);
  line-height: 1.6;
}
"""


def _console_error(message: str) -> None:
    """Emit ``console.error`` in the browser (no-op outside Pyodide)."""
    if "pyodide" in sys.modules:
        try:
            from js import console  # type: ignore[import-not-found]

            console.error(message)
        except ImportError:
            pass


def _error_banner(message: str) -> pn.pane.Alert:
    """Return a Panel pane rendering a warning banner."""
    return pn.pane.Alert(message, alert_type="warning")


def _schema_mismatch_banner(actual_version: str) -> pn.pane.Alert:
    """Return a banner for schema version mismatches (non-blocking)."""
    message = (
        f"Data schema version `{actual_version}` does not match "
        f"expected `{EXPECTED_SCHEMA_VERSION}`. The display may "
        f"be incorrect."
    )
    return pn.pane.Alert(message, alert_type="warning")


def _parse_iso_date(value: str) -> datetime | None:
    """Parse an ISO-8601 date string into a midnight ``datetime``.

    Returns ``None`` on failure so the caller can skip bad rows.

    Why ``datetime`` and not ``date``: Bokeh's datetime axis expects
    ``datetime.datetime`` values (which it converts to milliseconds
    since epoch). Plain ``datetime.date`` values trigger Bokeh's
    "could not set initial ranges" warning and the plot renders blank
    under its title.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _render_tracer_chart(rows: list[dict[str, Any]]) -> Any:
    """Render the tracer chart: fleet total generation over time.

    x-axis values are ``datetime`` (not ``date`` or ``str``) so Bokeh's
    datetime axis can auto-compute the data range.
    """
    from bokeh.plotting import figure

    if not rows:
        return pn.pane.Markdown("_No data available for the current range._")

    totals: dict[datetime, float] = {}
    for row in rows:
        parsed = _parse_iso_date(str(row.get("date", "")))
        if parsed is None:
            continue
        val = row.get("total_net_generation_mwh")
        if val is None:
            continue
        totals[parsed] = totals.get(parsed, 0.0) + float(val)

    if not totals:
        return pn.pane.Markdown("_No generation values found in payload._")

    sorted_dates = sorted(totals.keys())
    sorted_values = [totals[d] for d in sorted_dates]

    fig = figure(
        title="Fleet Total Net Generation — Daily",
        x_axis_label="Date",
        y_axis_label="MWh",
        x_axis_type="datetime",
        width=900,
        height=400,
    )
    fig.line(
        x=sorted_dates,
        y=sorted_values,
        line_color=_DATA_PRIMARY,
        line_width=2,
    )
    fig.scatter(
        x=sorted_dates,
        y=sorted_values,
        size=8,
        fill_color=_DATA_PRIMARY,
        line_color=_DATA_PRIMARY,
    )
    return pn.pane.Bokeh(fig)


async def build_body() -> pn.Column:
    """Fetch data and build the dashboard body (banners + chart)."""
    banners: list[Any] = []

    try:
        manifest = await load_manifest()
    except Exception as exc:
        _console_error(f"Failed to load manifest: {exc}")
        return pn.Column(
            _error_banner(
                "Data temporarily unavailable. Last successful refresh: "
                "never. Check back shortly."
            ),
            sizing_mode="stretch_width",
        )

    if not manifest.schema_matches:
        _console_error(
            f"Schema mismatch: got {manifest.schema_version}, "
            f"expected {EXPECTED_SCHEMA_VERSION}"
        )
        banners.append(_schema_mismatch_banner(manifest.schema_version))

    try:
        daily_df = await load_daily_performance()
    except Exception as exc:
        _console_error(f"Failed to load daily_performance.json: {exc}")
        return pn.Column(
            _error_banner(
                "Data temporarily unavailable. Refresh failed while "
                "loading daily performance metrics."
            ),
            sizing_mode="stretch_width",
        )

    daily_raw = daily_df.to_dicts()
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

pn.extension(sizing_mode="stretch_width", raw_css=[_PORTFOLIO_CSS])
# Apply the Bokeh theme to the current document so every figure in this
# app picks it up.
curdoc().theme = Theme(json=build_theme_json())

_header = pn.pane.Markdown(f"# {_DASHBOARD_TITLE}\n\n{_DASHBOARD_SUBTITLE}")
pn.Column(
    _header,
    pn.bind(build_body),
    sizing_mode="stretch_width",
).servable()
