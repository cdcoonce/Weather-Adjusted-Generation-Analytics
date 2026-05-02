"""Fleet Overview tab for the WAGA dashboard.

Exports:
- ``fleet_panel`` — reactive pn.Column with three charts
- Pure data-preparation functions (tested independently):
  - ``_apply_fleet_filters``
  - ``_prep_generation_lines``
  - ``_prep_capacity_bars``
  - ``_prep_heatmap``
  - ``_asset_color``

**Bundler note**: this file is inlined by ``scripts/build_dashboard_app.py``
before ``app.py`` is appended. Imports from ``weather_analytics.dashboard.*``
are stripped automatically.
"""

from datetime import datetime
from typing import Any

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
_FALLBACK_COLOR = "#888888"

# Per-asset palette for the generation line chart — desaturated, editorial.
# Keyed by position (index) so each asset gets a distinct colour regardless
# of type; up to 8 assets before cycling.
_ASSET_LINE_PALETTE: list[str] = [
    "#353535",  # charcoal
    "#4a7c7e",  # dusty teal
    "#a87c40",  # muted amber
    "#5a6e8a",  # steel blue
    "#6b5b72",  # dusty plum
    "#5a7a5a",  # sage
    "#8a5a4a",  # terracotta
    "#7a7a7a",  # stone
]

_MAX_HEATMAP_DAYS = 90


def _asset_color(asset_type: str) -> str:
    """Return the chart color for *asset_type* (case-insensitive).

    Parameters
    ----------
    asset_type : str
        Asset type string — ``"wind"`` / ``"Wind"``, ``"solar"`` / ``"Solar"``,
        or any other string.

    Returns
    -------
    str
        Hex color string.
    """
    normalized = asset_type.strip().lower()
    if normalized == "wind":
        return _WIND_COLOR
    if normalized == "solar":
        return _SOLAR_COLOR
    return _FALLBACK_COLOR


# ---------------------------------------------------------------------------
# Pure data-preparation helpers
# ---------------------------------------------------------------------------


def _apply_fleet_filters(
    daily_df: pl.DataFrame,
    assets_df: pl.DataFrame,
    asset_id: str,
    asset_type: str,
    date_start: str,
    date_end: str,
) -> pl.DataFrame:
    """Join daily_df with assets_df and apply all four filters.

    Parameters
    ----------
    daily_df : pl.DataFrame
        Daily performance rows (``asset_id``, ``date``, …).
    assets_df : pl.DataFrame
        Assets lookup table with ``asset_id`` and ``asset_type``.
    asset_id : str
        ``"All"`` or a specific asset identifier.
    asset_type : str
        ``"All"``, ``"Wind"``, or ``"Solar"``.
    date_start : str
        ISO-8601 start date (inclusive). Empty string = no lower bound.
    date_end : str
        ISO-8601 end date (inclusive). Empty string = no upper bound.

    Returns
    -------
    pl.DataFrame
        Filtered rows with ``asset_type`` column added via left join.
    """
    # Left-join to attach asset_type for color mapping.
    result = daily_df.join(
        assets_df.select(["asset_id", "asset_type"]),
        on="asset_id",
        how="left",
    )

    if asset_id != "All":
        result = result.filter(pl.col("asset_id") == asset_id)
    if asset_type != "All":
        result = result.filter(
            pl.col("asset_type").str.to_lowercase() == asset_type.lower()
        )
    if date_start:
        result = result.filter(pl.col("date") >= date_start)
    if date_end:
        result = result.filter(pl.col("date") <= date_end)

    return result


def _prep_generation_lines(
    df: pl.DataFrame,
) -> list[tuple[str, list[datetime], list[float], str]]:
    """Build per-asset line data for the generation-over-time chart.

    Parameters
    ----------
    df : pl.DataFrame
        Filtered daily DataFrame with ``asset_id``, ``date``,
        ``total_net_generation_mwh``, and ``asset_type`` columns.

    Returns
    -------
    list[tuple[str, list[datetime], list[float], str]]
        One entry per asset: ``(asset_id, dates, values, color)`` where
        *dates* are ``datetime`` objects (midnight) for Bokeh's datetime axis.
    """
    if df.is_empty():
        return []

    lines: list[tuple[str, list[datetime], list[float], str]] = []
    sorted_asset_ids = df["asset_id"].unique(maintain_order=False).sort().to_list()
    for idx, asset_id_val in enumerate(sorted_asset_ids):
        asset_rows = df.filter(pl.col("asset_id") == asset_id_val).sort("date")
        dates: list[datetime] = [
            datetime.fromisoformat(d) for d in asset_rows["date"].to_list()
        ]
        values: list[float] = asset_rows["total_net_generation_mwh"].to_list()
        color = _ASSET_LINE_PALETTE[idx % len(_ASSET_LINE_PALETTE)]
        lines.append((asset_id_val, dates, values, color))

    return lines


def _prep_capacity_bars(
    df: pl.DataFrame,
) -> tuple[list[str], list[float], list[str]]:
    """Compute mean capacity factor per asset, sorted descending.

    Parameters
    ----------
    df : pl.DataFrame
        Filtered daily DataFrame with ``asset_id``, ``daily_capacity_factor``,
        and ``asset_type`` columns.

    Returns
    -------
    tuple[list[str], list[float], list[str]]
        ``(asset_ids, mean_cfs, colors)`` — three parallel lists.
        Sorted so the highest mean CF is first.
    """
    if df.is_empty():
        return [], [], []

    agg = (
        df.group_by(["asset_id", "asset_type"])
        .agg(pl.col("daily_capacity_factor").mean().alias("mean_cf"))
        .sort("mean_cf", descending=True)
    )

    asset_ids: list[str] = agg["asset_id"].to_list()
    mean_cfs: list[float] = agg["mean_cf"].to_list()
    colors: list[str] = [_asset_color(t) for t in agg["asset_type"].to_list()]

    return asset_ids, mean_cfs, colors


def _prep_heatmap(
    weather_df: pl.DataFrame,
    assets_df: pl.DataFrame,
    asset_id: str,
    asset_type: str,
    date_start: str,
    date_end: str,
) -> pl.DataFrame:
    """Filter weather performance data for the heatmap chart.

    Caps the date range at ``_MAX_HEATMAP_DAYS`` most recent distinct dates
    to keep the chart readable.

    Parameters
    ----------
    weather_df : pl.DataFrame
        Weather performance rows (``asset_id``, ``date``, ``performance_score``).
    assets_df : pl.DataFrame
        Assets lookup table with ``asset_id`` and ``asset_type``.
    asset_id : str
        ``"All"`` or a specific asset identifier.
    asset_type : str
        ``"All"``, ``"Wind"``, or ``"Solar"``.
    date_start : str
        ISO-8601 start date (inclusive). Empty string = no lower bound.
    date_end : str
        ISO-8601 end date (inclusive). Empty string = no upper bound.

    Returns
    -------
    pl.DataFrame
        Filtered rows with columns ``asset_id``, ``date``,
        ``performance_score``, ``asset_type``.
    """
    result = weather_df.join(
        assets_df.select(["asset_id", "asset_type"]),
        on="asset_id",
        how="left",
    )

    if asset_id != "All":
        result = result.filter(pl.col("asset_id") == asset_id)
    if asset_type != "All":
        result = result.filter(
            pl.col("asset_type").str.to_lowercase() == asset_type.lower()
        )
    if date_start:
        result = result.filter(pl.col("date") >= date_start)
    if date_end:
        result = result.filter(pl.col("date") <= date_end)

    # Cap to most recent _MAX_HEATMAP_DAYS distinct dates.
    if not result.is_empty():
        all_dates = result["date"].unique().sort(descending=True)
        if all_dates.shape[0] > _MAX_HEATMAP_DAYS:
            recent_dates = all_dates.head(_MAX_HEATMAP_DAYS)
            result = result.filter(pl.col("date").is_in(recent_dates))

    return result.select(["asset_id", "date", "performance_score", "asset_type"])


# ---------------------------------------------------------------------------
# Chart renderers
# ---------------------------------------------------------------------------


def _render_generation_chart(df: pl.DataFrame) -> Any:
    """Render the generation-over-time line chart from a filtered DataFrame."""
    import panel as pn

    lines = _prep_generation_lines(df)
    if not lines:
        return pn.pane.Markdown("_No generation data available._")

    fig = make_themed_figure(
        "Fleet Net Generation — Daily",
        "Date",
        "MWh",
        x_axis_type="datetime",
        height=350,
    )
    style_tooltip(
        fig,
        [("Date", "@x{%F}"), ("Asset", "@asset_id"), ("MWh", "@y{0.0}")],
    )
    # Update hover to format datetime
    from bokeh.models import HoverTool

    for tool in fig.tools:
        if isinstance(tool, HoverTool):
            tool.formatters = {"@x": "datetime"}

    for asset_id_val, dates, values, color in lines:
        source_data = {
            "x": dates,
            "y": values,
            "asset_id": [asset_id_val] * len(dates),
        }
        fig.line(
            x="x",
            y="y",
            source=source_data,
            line_color=color,
            line_width=2,
            legend_label=asset_id_val,
        )
        fig.scatter(
            x="x",
            y="y",
            source=source_data,
            size=6,
            fill_color=color,
            line_color=color,
            marker="circle",
        )

    fig.legend.location = "top_left"
    fig.legend.click_policy = "hide"
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


def _render_capacity_chart(df: pl.DataFrame) -> Any:
    """Render the horizontal capacity factor bar chart."""
    import panel as pn

    asset_ids, mean_cfs, colors = _prep_capacity_bars(df)
    if not asset_ids:
        return pn.pane.Markdown("_No capacity factor data available._")

    fig = make_themed_figure(
        "Average Capacity Factor by Asset",
        "Mean Capacity Factor",
        "Asset",
        y_range=asset_ids,
        height=max(200, len(asset_ids) * 40),
    )
    style_tooltip(fig, [("Asset", "@asset_id"), ("Mean CF", "@right{0.000}")])

    source_data = {
        "asset_id": asset_ids,
        "right": mean_cfs,
        "color": colors,
    }
    fig.hbar(
        y="asset_id",
        right="right",
        height=0.6,
        fill_color="color",
        line_color="color",
        source=source_data,
    )
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


def _render_heatmap_chart(df: pl.DataFrame) -> Any:
    """Render the performance score heatmap.

    Uses a real datetime x-axis (not categorical) so Bokeh auto-spaces tick
    labels at sensible intervals (~8 labels) without crowding.
    """
    import panel as pn
    from bokeh.models import ColorBar, DatetimeTickFormatter, LinearColorMapper, Range1d
    from bokeh.palettes import Viridis256
    from bokeh.transform import transform

    asset_ids = df["asset_id"].unique().sort().to_list()

    # Parse date strings to datetime objects (strip nanosecond precision).
    def _to_dt(s: str) -> datetime:
        return datetime.fromisoformat(s[:10])

    day_ms = 24 * 60 * 60 * 1000  # milliseconds per day

    date_strs: list[str] = df["date"].to_list()
    date_ms: list[float] = [_to_dt(s).timestamp() * 1000 for s in date_strs]

    all_dates_dt = [_to_dt(s) for s in df["date"].unique().sort().to_list()]
    x_start = min(all_dates_dt).timestamp() * 1000 - day_ms / 2
    x_end = max(all_dates_dt).timestamp() * 1000 + day_ms / 2

    mapper = LinearColorMapper(
        palette=Viridis256,
        low=float(df["performance_score"].min() or 0.0),
        high=float(df["performance_score"].max() or 1.0),
    )

    fig = make_themed_figure(
        "Performance Score Heatmap",
        "Date",
        "Asset",
        x_range=Range1d(x_start, x_end),
        y_range=asset_ids,
        x_axis_type="datetime",
        height=max(200, len(asset_ids) * 40),
    )
    style_tooltip(
        fig,
        [
            ("Asset", "@asset_id"),
            ("Date", "@date_str"),
            ("Score", "@performance_score{0.000}"),
        ],
    )

    fig.xaxis.formatter = DatetimeTickFormatter(days="%b %d", months="%b %Y")
    fig.xaxis.major_label_orientation = 0.9

    source_data = {
        "asset_id": df["asset_id"].to_list(),
        "date": date_ms,
        "date_str": [s[:10] for s in date_strs],
        "performance_score": df["performance_score"].to_list(),
    }
    fig.rect(
        x="date",
        y="asset_id",
        width=day_ms * 0.95,
        height=1,
        source=source_data,
        fill_color=transform("performance_score", mapper),
        line_color=None,
    )

    color_bar = ColorBar(color_mapper=mapper, width=8)
    fig.add_layout(color_bar, "right")

    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


# ---------------------------------------------------------------------------
# Public component
# ---------------------------------------------------------------------------


def fleet_panel(filters: Any) -> Any:
    """Build a reactive pn.Column with three Fleet Overview charts.

    All three charts re-render whenever any filter parameter changes.

    Parameters
    ----------
    filters : Filters
        Populated ``Filters`` instance with ``_daily_df``, ``_weather_df``,
        and ``_assets_df`` attributes set.

    Returns
    -------
    pn.Column
        Column containing the three reactive chart panels.
    """
    import panel as pn

    @pn.depends(
        filters.param.asset_id,
        filters.param.asset_type,
        filters.param.date_start,
        filters.param.date_end,
    )
    def _generation_chart(
        asset_id: str,
        asset_type: str,
        date_start: str,
        date_end: str,
    ) -> Any:
        _raw_daily = getattr(filters, "_daily_df", None)
        daily_df: pl.DataFrame = (
            _raw_daily if _raw_daily is not None else pl.DataFrame()
        )
        _raw_assets = getattr(filters, "_assets_df", None)
        assets_df: pl.DataFrame = (
            _raw_assets if _raw_assets is not None else pl.DataFrame()
        )
        filtered = _apply_fleet_filters(
            daily_df, assets_df, asset_id, asset_type, date_start, date_end
        )
        return with_empty_guard(
            filtered,
            _render_generation_chart,
            message="No generation data for the selected filters.",
        )

    @pn.depends(
        filters.param.asset_id,
        filters.param.asset_type,
        filters.param.date_start,
        filters.param.date_end,
    )
    def _capacity_chart(
        asset_id: str,
        asset_type: str,
        date_start: str,
        date_end: str,
    ) -> Any:
        _raw_daily = getattr(filters, "_daily_df", None)
        daily_df: pl.DataFrame = (
            _raw_daily if _raw_daily is not None else pl.DataFrame()
        )
        _raw_assets = getattr(filters, "_assets_df", None)
        assets_df: pl.DataFrame = (
            _raw_assets if _raw_assets is not None else pl.DataFrame()
        )
        filtered = _apply_fleet_filters(
            daily_df, assets_df, asset_id, asset_type, date_start, date_end
        )
        return with_empty_guard(
            filtered,
            _render_capacity_chart,
            message="No capacity factor data for the selected filters.",
        )

    @pn.depends(
        filters.param.asset_id,
        filters.param.asset_type,
        filters.param.date_start,
        filters.param.date_end,
    )
    def _heatmap_chart(
        asset_id: str,
        asset_type: str,
        date_start: str,
        date_end: str,
    ) -> Any:
        _raw_weather = getattr(filters, "_weather_df", None)
        weather_df: pl.DataFrame = (
            _raw_weather if _raw_weather is not None else pl.DataFrame()
        )
        _raw_assets = getattr(filters, "_assets_df", None)
        assets_df: pl.DataFrame = (
            _raw_assets if _raw_assets is not None else pl.DataFrame()
        )
        heatmap_df = _prep_heatmap(
            weather_df, assets_df, asset_id, asset_type, date_start, date_end
        )
        return with_empty_guard(
            heatmap_df,
            _render_heatmap_chart,
            message="No performance score data for the selected filters.",
        )

    return pn.Column(
        pn.panel(_generation_chart),
        pn.panel(_capacity_chart),
        pn.panel(_heatmap_chart),
        sizing_mode="stretch_width",
    )
