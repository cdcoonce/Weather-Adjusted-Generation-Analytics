"""WAGA dashboard Panel app — Phase 1 tracer bullet.

Renders a minimal single-chart dashboard that proves the end-to-end
pipeline (Dagster -> JSON -> portfolio repo -> Pyodide -> Panel -> Bokeh)
works in a real browser. Later dev-cycle phases expand this into the
full three-tab dashboard with filter bar, KPI row, and reactive components.

Architecture notes
------------------
- The Bokeh theme is applied INSIDE ``build_app()`` (which is called by
  ``servable()``), not at module import. Applying at import would mutate
  Panel's module-level config and pollute tests that import this module.
- Data is fetched asynchronously via ``data_loader.load_*`` which in
  turn uses ``pyodide.http.pyfetch`` (browser) or ``urllib`` (local dev).
- On fetch failure, the app renders a banner instead of crashing.
- On schema version mismatch, the app renders a warning banner but
  still attempts to render the chart (non-blocking).
- ``console.error`` is emitted for every browser-visible error so
  operators can see them in the browser DevTools console.
"""

from typing import Any

from weather_analytics.dashboard.data_loader import (
    EXPECTED_SCHEMA_VERSION,
    Manifest,
    clear_cache,
    load_daily_performance,
    load_manifest,
)
from weather_analytics.dashboard.theme import DATA_PALETTE, load_theme

_DASHBOARD_TITLE = "Weather-Adjusted Generation Analytics"
_DASHBOARD_SUBTITLE = (
    "Renewable asset performance with weather-adjusted correlations — "
    "Phase 1 tracer bullet."
)


def _console_error(message: str) -> None:
    """Emit a ``console.error`` in the browser (no-op outside Pyodide).

    Uses Pyodide's JS bridge so errors are visible in DevTools. Falls
    back silently outside Pyodide so tests and local runs aren't noisy.
    """
    import sys

    if "pyodide" in sys.modules:
        try:
            from js import console  # type: ignore[import-not-found]

            console.error(message)
        except ImportError:
            pass


def _error_banner(message: str) -> Any:
    """Return a Panel pane rendering a dismissible error banner."""
    import panel as pn

    return pn.pane.Alert(message, alert_type="warning")


def _schema_mismatch_banner(manifest: Manifest) -> Any:
    """Return a banner for schema version mismatches."""
    import panel as pn

    message = (
        f"Data schema version `{manifest.schema_version}` does not match "
        f"expected `{EXPECTED_SCHEMA_VERSION}`. The display may be incorrect."
    )
    return pn.pane.Alert(message, alert_type="warning")


def _render_tracer_chart(daily_df: Any) -> Any:
    """Render the Phase 1 tracer chart: total generation over time.

    Parameters
    ----------
    daily_df : polars.DataFrame
        Daily performance data with at least ``date`` and
        ``total_net_generation_mwh`` columns.

    Returns
    -------
    panel.pane.Bokeh
        Chart wrapped in a Panel pane ready for layout inclusion.
    """
    import panel as pn
    import polars as pl
    from bokeh.plotting import figure

    if daily_df.is_empty():
        return pn.pane.Markdown("_No data for the selected range._")

    # Aggregate across assets so the tracer chart is fleet-wide.
    agg = (
        daily_df.lazy()
        .group_by("date")
        .agg(pl.col("total_net_generation_mwh").sum().alias("total_mwh"))
        .sort("date")
        .collect()
    )

    fig = figure(
        title="Fleet Total Net Generation — Daily",
        x_axis_label="Date",
        y_axis_label="MWh",
        x_axis_type="datetime",
        sizing_mode="stretch_width",
        height=360,
        tools="",  # Toolbar hidden for minimal styling (Phase 1 default).
        toolbar_location=None,
    )
    fig.line(
        x=agg["date"].to_list(),
        y=agg["total_mwh"].to_list(),
        line_color=DATA_PALETTE["primary"],
        line_width=2,
    )
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


def _layout_chrome(title: str, subtitle: str, body: Any) -> Any:
    """Return the top-level layout (header + body)."""
    import panel as pn

    header = pn.pane.Markdown(f"# {title}\n\n{subtitle}")
    return pn.Column(header, body, sizing_mode="stretch_width")


async def build_app() -> Any:
    """Construct the Panel application.

    This function is the single place where the Bokeh theme is applied
    and where data is fetched. Keeping all side effects inside this
    function (as opposed to at module import) means tests can import
    the module without triggering Panel config mutations.

    Returns
    -------
    panel.layout.Column
        The assembled dashboard layout, ready to be ``.servable()``.
    """
    import panel as pn

    # Ensure Panel is initialized and theme is applied exactly once.
    pn.extension(sizing_mode="stretch_width")
    pn.config.theme = load_theme()

    banners: list[Any] = []

    try:
        manifest = await load_manifest()
    except Exception as exc:
        _console_error(f"Failed to load manifest: {exc}")
        return _layout_chrome(
            _DASHBOARD_TITLE,
            _DASHBOARD_SUBTITLE,
            _error_banner(
                "Data temporarily unavailable. Last successful refresh: never. "
                "Check back shortly."
            ),
        )

    if not manifest.schema_matches:
        _console_error(
            f"Schema mismatch: got {manifest.schema_version}, "
            f"expected {EXPECTED_SCHEMA_VERSION}"
        )
        banners.append(_schema_mismatch_banner(manifest))

    try:
        daily_df = await load_daily_performance()
    except Exception as exc:
        _console_error(f"Failed to load daily_performance.json: {exc}")
        return _layout_chrome(
            _DASHBOARD_TITLE,
            _DASHBOARD_SUBTITLE,
            _error_banner(
                "Data temporarily unavailable. Refresh failed while loading "
                "daily performance metrics."
            ),
        )

    chart = _render_tracer_chart(daily_df)
    body = pn.Column(*banners, chart, sizing_mode="stretch_width")
    return _layout_chrome(_DASHBOARD_TITLE, _DASHBOARD_SUBTITLE, body)


def servable() -> Any:
    """Panel entry point used by ``panel convert``.

    Returns
    -------
    panel.viewable.Viewable
        Async-materialized app instance.
    """
    import panel as pn

    # Ensure a clean data cache for fresh loads (important for panel serve).
    clear_cache()
    return pn.panel(build_app())


# ``panel convert`` and ``panel serve`` both look for a top-level
# ``.servable()`` call. Keep this at module bottom so both tooling paths
# find it without needing a ``__main__`` guard.
if "panel" in __import__("sys").modules:
    servable().servable()
