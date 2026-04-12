"""Asset Deep-Dive tab for the WAGA dashboard.

Exports:
- ``asset_panel`` — reactive pn.Column with four charts for a single asset
- Pure data-preparation functions (tested independently):
  - ``_filter_asset_daily``
  - ``_filter_asset_weather``
  - ``_get_asset_type``
  - ``_prep_expected_vs_actual``
  - ``_prep_rolling_cf``
  - ``_prep_scatter``
  - ``_prep_stacked_hours``
  - ``_fit_regression``

**Bundler note**: this file is inlined by ``scripts/build_dashboard_app.py``
before ``app.py`` is appended. Imports from ``weather_analytics.dashboard.*``
are stripped automatically.
"""

from datetime import datetime
from typing import Any

import numpy as np
import polars as pl

from weather_analytics.dashboard.components._chart_helpers import (
    make_themed_figure,
    style_tooltip,
    with_empty_guard,
)

# ---------------------------------------------------------------------------
# Palette
# ---------------------------------------------------------------------------

_WIND_COLOR = "#4a7c7e"
_SOLAR_COLOR = "#d4a44c"
_ACTUAL_COLOR = "#353535"
_EXPECTED_COLOR = "#999999"
_CF_7D_COLOR = "#4a7c7e"
_CF_30D_COLOR = "#353535"
_CF_RAW_COLOR = "#cccccc"

HOUR_COLS = ["excellent_hours", "good_hours", "fair_hours", "poor_hours"]
HOUR_COLORS = ["#2d6a4f", "#52b788", "#f4a261", "#e76f51"]

_MIN_REGRESSION_POINTS = 2


# ---------------------------------------------------------------------------
# Pure data-preparation helpers
# ---------------------------------------------------------------------------


def _filter_asset_daily(
    daily_df: pl.DataFrame,
    asset_id: str,
    date_start: str,
    date_end: str,
) -> pl.DataFrame:
    """Filter daily_df to a single asset and date range.

    Parameters
    ----------
    daily_df : pl.DataFrame
        Daily performance rows.
    asset_id : str
        Specific asset identifier (not ``"All"``).
    date_start : str
        ISO-8601 start date (inclusive). Empty string = no lower bound.
    date_end : str
        ISO-8601 end date (inclusive). Empty string = no upper bound.

    Returns
    -------
    pl.DataFrame
        Filtered and sorted rows for the given asset.
    """
    result = daily_df.filter(pl.col("asset_id") == asset_id)
    if date_start:
        result = result.filter(pl.col("date") >= date_start)
    if date_end:
        result = result.filter(pl.col("date") <= date_end)
    return result.sort("date")


def _filter_asset_weather(
    weather_df: pl.DataFrame,
    asset_id: str,
    date_start: str,
    date_end: str,
) -> pl.DataFrame:
    """Filter weather_df to a single asset and date range.

    Parameters
    ----------
    weather_df : pl.DataFrame
        Weather performance rows.
    asset_id : str
        Specific asset identifier (not ``"All"``).
    date_start : str
        ISO-8601 start date (inclusive). Empty string = no lower bound.
    date_end : str
        ISO-8601 end date (inclusive). Empty string = no upper bound.

    Returns
    -------
    pl.DataFrame
        Filtered and sorted rows for the given asset.
    """
    result = weather_df.filter(pl.col("asset_id") == asset_id)
    if date_start:
        result = result.filter(pl.col("date") >= date_start)
    if date_end:
        result = result.filter(pl.col("date") <= date_end)
    return result.sort("date")


def _get_asset_type(weather_df: pl.DataFrame) -> str:
    """Return ``inferred_asset_type`` from the first row, or empty string.

    Parameters
    ----------
    weather_df : pl.DataFrame
        Already-filtered weather performance rows for a single asset.

    Returns
    -------
    str
        Asset type string (e.g. ``"Wind"`` or ``"Solar"``), or ``""`` if
        the DataFrame is empty.
    """
    if weather_df.is_empty():
        return ""
    return str(weather_df["inferred_asset_type"][0])


def _prep_expected_vs_actual(
    weather_df: pl.DataFrame,
) -> tuple[list[datetime], list[float], list[float]]:
    """Extract dates, actual MWh, and expected MWh series from weather_df.

    Parameters
    ----------
    weather_df : pl.DataFrame
        Filtered weather performance rows (pre-sorted by date).

    Returns
    -------
    tuple[list[datetime], list[float], list[float]]
        ``(dates, actual_mwh, expected_mwh)`` — three parallel lists sorted
        by date. Dates are ``datetime`` objects (midnight) for Bokeh's
        datetime axis.
    """
    if weather_df.is_empty():
        return [], [], []

    df = weather_df.sort("date")
    dates: list[datetime] = [datetime.fromisoformat(d) for d in df["date"].to_list()]
    actual: list[float] = df["avg_actual_generation_mwh"].to_list()
    expected: list[float] = df["avg_expected_generation_mwh"].to_list()
    return dates, actual, expected


def _prep_rolling_cf(
    weather_df: pl.DataFrame,
    daily_df: pl.DataFrame,
) -> tuple[list[datetime], list[float], list[float], list[float]]:
    """Extract dates and three capacity factor series.

    Uses weather_df dates as the primary axis. Joins raw ``daily_capacity_factor``
    from daily_df on the ``date`` column.

    Parameters
    ----------
    weather_df : pl.DataFrame
        Filtered weather performance rows (pre-sorted by date).
    daily_df : pl.DataFrame
        Filtered daily performance rows for the same asset.

    Returns
    -------
    tuple[list[datetime], list[float], list[float], list[float]]
        ``(dates, cf_7d, cf_30d, raw_cf)`` — four parallel lists sorted by
        date. Dates are ``datetime`` objects.
    """
    if weather_df.is_empty():
        return [], [], [], []

    df = weather_df.sort("date")

    # Join raw CF from daily_df.
    if not daily_df.is_empty():
        daily_cf = daily_df.select(["date", "daily_capacity_factor"]).sort("date")
        df = df.join(daily_cf, on="date", how="left")
        raw_cf: list[float] = df["daily_capacity_factor"].to_list()
    else:
        raw_cf = [0.0] * df.shape[0]

    dates: list[datetime] = [datetime.fromisoformat(d) for d in df["date"].to_list()]
    cf_7d: list[float] = df["rolling_7d_avg_cf"].to_list()
    cf_30d: list[float] = df["rolling_30d_avg_cf"].to_list()
    return dates, cf_7d, cf_30d, raw_cf


def _prep_scatter(
    daily_df: pl.DataFrame,
    asset_type: str,
) -> tuple[list[float], list[float], float]:
    """Extract scatter plot data (weather var vs generation) with r-squared.

    Parameters
    ----------
    daily_df : pl.DataFrame
        Filtered daily performance rows.
    asset_type : str
        ``"Wind"`` or ``"Solar"`` — determines which weather variable to use.

    Returns
    -------
    tuple[list[float], list[float], float]
        ``(x_vals, y_vals, r_squared)`` where *x_vals* are the weather
        variable values (wind speed or GHI), *y_vals* are generation MWh,
        and *r_squared* is computed from ``numpy.polyfit`` residuals.
    """
    if daily_df.is_empty():
        return [], [], 0.0

    df = daily_df.sort("date")
    y_vals: list[float] = df["total_net_generation_mwh"].to_list()

    if asset_type == "Solar":
        x_vals: list[float] = df["avg_ghi"].to_list()
    else:
        x_vals = df["avg_wind_speed_mps"].to_list()

    if len(x_vals) < _MIN_REGRESSION_POINTS:
        return x_vals, y_vals, 0.0

    x_arr = np.array(x_vals)
    y_arr = np.array(y_vals)
    coeffs = np.polyfit(x_arr, y_arr, 1)
    y_pred = np.polyval(coeffs, x_arr)
    ss_res = float(np.sum((y_arr - y_pred) ** 2))
    ss_tot = float(np.sum((y_arr - y_arr.mean()) ** 2))
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0.0 else 0.0

    return x_vals, y_vals, max(0.0, r_squared)


def _prep_stacked_hours(daily_df: pl.DataFrame) -> pl.DataFrame:
    """Return a DataFrame with the date and four hour-category columns.

    Parameters
    ----------
    daily_df : pl.DataFrame
        Filtered daily performance rows.

    Returns
    -------
    pl.DataFrame
        Subset with columns ``date``, ``excellent_hours``, ``good_hours``,
        ``fair_hours``, ``poor_hours``, sorted by date.
    """
    cols = ["date", *HOUR_COLS]
    if daily_df.is_empty():
        return pl.DataFrame(
            {
                c: pl.Series([], dtype=pl.Utf8 if c == "date" else pl.Float64)
                for c in cols
            }
        )
    return daily_df.select(cols).sort("date")


def _fit_regression(
    x_vals: list[float],
    y_vals: list[float],
) -> tuple[list[float], list[float]]:
    """Return ``(x_line, y_line)`` for a linear regression overlay.

    Parameters
    ----------
    x_vals : list[float]
        Independent variable values.
    y_vals : list[float]
        Dependent variable values.

    Returns
    -------
    tuple[list[float], list[float]]
        ``(x_sorted, y_line)`` — x values sorted ascending with corresponding
        regression-line y values.
    """
    if len(x_vals) < _MIN_REGRESSION_POINTS:
        return x_vals, y_vals
    coeffs = np.polyfit(x_vals, y_vals, 1)
    x_sorted = sorted(x_vals)
    y_line = [float(coeffs[0] * x + coeffs[1]) for x in x_sorted]
    return x_sorted, y_line


# ---------------------------------------------------------------------------
# Chart renderers
# ---------------------------------------------------------------------------


def _render_expected_vs_actual(df: pl.DataFrame, asset_id: str) -> Any:
    """Render expected vs actual generation line chart."""
    import panel as pn

    dates, actual, expected = _prep_expected_vs_actual(df)
    if not dates:
        return pn.pane.Markdown("_No expected vs. actual data available._")

    fig = make_themed_figure(
        f"Expected vs. Actual Generation \u2014 {asset_id}",
        "Date",
        "MWh",
        x_axis_type="datetime",
        height=350,
    )
    style_tooltip(
        fig,
        [
            ("Date", "@x{%F}"),
            ("Actual MWh", "@actual{0.0}"),
            ("Expected MWh", "@expected{0.0}"),
        ],
    )
    from bokeh.models import HoverTool

    for tool in fig.tools:
        if isinstance(tool, HoverTool):
            tool.formatters = {"@x": "datetime"}

    fig.line(
        x=dates,
        y=actual,
        line_color=_ACTUAL_COLOR,
        line_width=2,
        legend_label="Actual",
    )
    fig.line(
        x=dates,
        y=expected,
        line_color=_EXPECTED_COLOR,
        line_width=2,
        line_dash="dashed",
        legend_label="Expected",
    )
    fig.legend.location = "top_left"
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


def _render_rolling_cf(
    weather_df: pl.DataFrame,
    daily_df: pl.DataFrame,
    asset_id: str,
) -> Any:
    """Render rolling capacity factor line chart."""
    import panel as pn

    dates, cf_7d, cf_30d, raw_cf = _prep_rolling_cf(weather_df, daily_df)
    if not dates:
        return pn.pane.Markdown("_No capacity factor data available._")

    fig = make_themed_figure(
        f"Capacity Factor Trends \u2014 {asset_id}",
        "Date",
        "Capacity Factor",
        x_axis_type="datetime",
        height=350,
    )
    style_tooltip(
        fig,
        [
            ("Date", "@x{%F}"),
            ("7d CF", "@cf7{0.000}"),
            ("30d CF", "@cf30{0.000}"),
        ],
    )
    from bokeh.models import HoverTool

    for tool in fig.tools:
        if isinstance(tool, HoverTool):
            tool.formatters = {"@x": "datetime"}

    fig.line(
        x=dates,
        y=raw_cf,
        line_color=_CF_RAW_COLOR,
        line_width=1,
        legend_label="Raw CF",
    )
    fig.line(
        x=dates,
        y=cf_7d,
        line_color=_CF_7D_COLOR,
        line_width=2,
        legend_label="7d Avg CF",
    )
    fig.line(
        x=dates,
        y=cf_30d,
        line_color=_CF_30D_COLOR,
        line_width=2.5,
        legend_label="30d Avg CF",
    )
    fig.legend.location = "top_left"
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


def _render_scatter(daily_df: pl.DataFrame, asset_type: str) -> Any:
    """Render weather variable vs generation scatter with regression line."""
    import panel as pn

    x_vals, y_vals, r2 = _prep_scatter(daily_df, asset_type)
    if not x_vals:
        return pn.pane.Markdown("_No scatter data available._")

    if asset_type == "Solar":
        x_label = "GHI (W/m²)"
        color = _SOLAR_COLOR
        title = f"GHI vs. Generation (R\u00b2={r2:.3f})"
    else:
        x_label = "Wind Speed (m/s)"
        color = _WIND_COLOR
        title = f"Wind Speed vs. Generation (R\u00b2={r2:.3f})"

    fig = make_themed_figure(
        title,
        x_label,
        "MWh",
        height=350,
    )
    style_tooltip(
        fig,
        [
            (x_label, "@x{0.0}"),
            ("MWh", "@y{0.0}"),
        ],
    )

    fig.scatter(
        x=x_vals,
        y=y_vals,
        size=8,
        fill_color=color,
        line_color=color,
        alpha=0.7,
    )

    if len(x_vals) >= _MIN_REGRESSION_POINTS:
        x_line, y_line = _fit_regression(x_vals, y_vals)
        fig.line(
            x=x_line,
            y=y_line,
            line_color=_ACTUAL_COLOR,
            line_width=2,
            line_dash="dashed",
        )

    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


def _render_stacked_hours(daily_df: pl.DataFrame, asset_id: str) -> Any:
    """Render stacked bar chart of performance distribution hours."""
    import panel as pn
    from bokeh.models import ColumnDataSource

    hours_df = _prep_stacked_hours(daily_df)
    if hours_df.is_empty():
        return pn.pane.Markdown("_No performance hours data available._")

    dates: list[datetime] = [
        datetime.fromisoformat(d) for d in hours_df["date"].to_list()
    ]

    # Compute bar width: one day in milliseconds * 0.8.
    bar_width_ms = 0.8 * 24 * 60 * 60 * 1000

    source = ColumnDataSource(
        {
            "x": dates,
            "excellent_hours": hours_df["excellent_hours"].to_list(),
            "good_hours": hours_df["good_hours"].to_list(),
            "fair_hours": hours_df["fair_hours"].to_list(),
            "poor_hours": hours_df["poor_hours"].to_list(),
        }
    )

    fig = make_themed_figure(
        f"Performance Distribution \u2014 {asset_id}",
        "Date",
        "Hours",
        x_axis_type="datetime",
        height=350,
    )
    style_tooltip(
        fig,
        [
            ("Date", "@x{%F}"),
            ("Excellent", "@excellent_hours{0.0}"),
            ("Good", "@good_hours{0.0}"),
            ("Fair", "@fair_hours{0.0}"),
            ("Poor", "@poor_hours{0.0}"),
        ],
    )
    from bokeh.models import HoverTool

    for tool in fig.tools:
        if isinstance(tool, HoverTool):
            tool.formatters = {"@x": "datetime"}

    fig.vbar_stack(
        HOUR_COLS,
        x="x",
        width=bar_width_ms,
        color=HOUR_COLORS,
        source=source,
        legend_label=HOUR_COLS,
    )
    fig.legend.location = "top_left"
    fig.legend.click_policy = "hide"
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


# ---------------------------------------------------------------------------
# Public component
# ---------------------------------------------------------------------------

_PLACEHOLDER = "_Select an asset from the filter bar to view its deep-dive metrics._"


def asset_panel(filters: Any) -> Any:
    """Build a reactive pn.Column with four Asset Deep-Dive charts.

    When ``filters.asset_id == "All"``, renders a Markdown placeholder
    asking the user to select an asset. Otherwise renders four charts:
    expected vs actual generation, rolling capacity factor, scatter plot,
    and stacked performance hours.

    All charts re-render when ``asset_id``, ``date_start``, or ``date_end``
    changes.

    Parameters
    ----------
    filters : Filters
        Populated ``Filters`` instance with ``_daily_df`` and ``_weather_df``
        attributes set.

    Returns
    -------
    pn.Column
        Column containing the reactive chart panels.
    """
    import panel as pn

    @pn.depends(
        filters.param.asset_id,
        filters.param.date_start,
        filters.param.date_end,
    )
    def _charts(
        asset_id: str,
        date_start: str,
        date_end: str,
    ) -> Any:
        if asset_id == "All":
            return pn.pane.Markdown(_PLACEHOLDER)

        _raw_daily = getattr(filters, "_daily_df", None)
        daily_df: pl.DataFrame = (
            _raw_daily if _raw_daily is not None else pl.DataFrame()
        )
        _raw_weather = getattr(filters, "_weather_df", None)
        weather_df: pl.DataFrame = (
            _raw_weather if _raw_weather is not None else pl.DataFrame()
        )

        daily_filtered = (
            _filter_asset_daily(daily_df, asset_id, date_start, date_end)
            if not daily_df.is_empty()
            else daily_df
        )
        weather_filtered = (
            _filter_asset_weather(weather_df, asset_id, date_start, date_end)
            if not weather_df.is_empty()
            else weather_df
        )

        asset_type = _get_asset_type(weather_filtered)

        chart1 = with_empty_guard(
            weather_filtered,
            lambda df: _render_expected_vs_actual(df, asset_id),
            message="No expected vs. actual data for the selected filters.",
        )
        chart2 = with_empty_guard(
            weather_filtered,
            lambda df: _render_rolling_cf(df, daily_filtered, asset_id),
            message="No capacity factor data for the selected filters.",
        )
        chart3 = with_empty_guard(
            daily_filtered,
            lambda df: _render_scatter(df, asset_type),
            message="No scatter data for the selected filters.",
        )
        chart4 = with_empty_guard(
            daily_filtered,
            lambda df: _render_stacked_hours(df, asset_id),
            message="No performance hours data for the selected filters.",
        )

        return pn.Column(chart1, chart2, chart3, chart4, sizing_mode="stretch_width")

    return pn.Column(
        pn.panel(_charts),
        sizing_mode="stretch_width",
    )
