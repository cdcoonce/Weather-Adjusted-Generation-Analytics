"""Weather Correlation tab for the WAGA dashboard.

Exports:
- ``weather_panel`` — reactive pn.Column with three weather correlation charts
- Pure data-preparation functions (tested independently):
  - ``_prep_r2_bars``
  - ``_prep_wind_scatter``
  - ``_prep_solar_scatter``

**Bundler note**: this file is inlined by ``scripts/build_dashboard_app.py``
before ``app.py`` is appended. Imports from ``weather_analytics.dashboard.*``
are stripped automatically.
"""

from typing import Any

import polars as pl

from weather_analytics.dashboard.components._chart_helpers import (
    make_themed_figure,
    style_tooltip,
    with_empty_guard,
)

# ---------------------------------------------------------------------------
# Palettes
# ---------------------------------------------------------------------------

_WIND_PALETTE = ["#4a7c7e", "#5ba3a6", "#2c5f61", "#7bc4c7", "#1a3d3f"]
_SOLAR_PALETTE = ["#d4a44c", "#e8c17a", "#b8892f", "#f0d9a0", "#8a6420"]

_WIND_R2_COLOR = "#4a7c7e"
_SOLAR_R2_COLOR = "#d4a44c"


# ---------------------------------------------------------------------------
# Pure data-preparation helpers
# ---------------------------------------------------------------------------


def _prep_r2_bars(
    weather_df: pl.DataFrame,
    assets_df: pl.DataFrame,
    asset_id: str,
    asset_type: str,
    date_start: str,
    date_end: str,
) -> pl.DataFrame:
    """Aggregate wind_r_squared and solar_r_squared per asset from weather_df.

    Parameters
    ----------
    weather_df : pl.DataFrame
        Weather performance rows (asset_id, date, wind_r_squared,
        solar_r_squared, inferred_asset_type).
    assets_df : pl.DataFrame
        Asset metadata with asset_id and asset_type columns.
    asset_id : str
        If not ``"All"``, filter to this specific asset.
    asset_type : str
        If not ``"All"``, filter to assets of this type via assets_df join.
    date_start : str
        ISO-8601 start date (inclusive). Empty string = no lower bound.
    date_end : str
        ISO-8601 end date (inclusive). Empty string = no upper bound.

    Returns
    -------
    pl.DataFrame
        One row per asset with columns: ``asset_id``, ``mean_wind_r2``,
        ``mean_solar_r2``. Empty if weather_df is empty.
    """
    _empty = pl.DataFrame(
        {
            "asset_id": pl.Series([], dtype=pl.Utf8),
            "mean_wind_r2": pl.Series([], dtype=pl.Float64),
            "mean_solar_r2": pl.Series([], dtype=pl.Float64),
        }
    )

    if weather_df.is_empty():
        return _empty

    df = weather_df
    if date_start:
        df = df.filter(pl.col("date") >= date_start)
    if date_end:
        df = df.filter(pl.col("date") <= date_end)

    if asset_id != "All":
        df = df.filter(pl.col("asset_id") == asset_id)

    if asset_type != "All" and not assets_df.is_empty():
        valid_ids = assets_df.filter(
            pl.col("asset_type").str.to_lowercase() == asset_type.lower()
        )["asset_id"]
        df = df.filter(pl.col("asset_id").is_in(valid_ids))

    if df.is_empty():
        return _empty

    result = df.group_by("asset_id").agg(
        pl.col("wind_r_squared").mean().alias("mean_wind_r2"),
        pl.col("solar_r_squared").mean().alias("mean_solar_r2"),
    )
    return result.sort("asset_id")


def _prep_wind_scatter(
    daily_df: pl.DataFrame,
    assets_df: pl.DataFrame,
    asset_id: str,
    date_start: str,
    date_end: str,
) -> pl.DataFrame:
    """Filter daily_df to wind assets for the wind speed vs generation scatter.

    Parameters
    ----------
    daily_df : pl.DataFrame
        Daily performance rows (asset_id, date, avg_wind_speed_mps,
        total_net_generation_mwh, ...).
    assets_df : pl.DataFrame
        Asset metadata with asset_id and asset_type columns.
    asset_id : str
        If not ``"All"``, filter to this specific asset (must be a wind asset
        for any rows to be returned).
    date_start : str
        ISO-8601 start date (inclusive). Empty string = no lower bound.
    date_end : str
        ISO-8601 end date (inclusive). Empty string = no upper bound.

    Returns
    -------
    pl.DataFrame
        Rows for wind assets with columns: ``asset_id``, ``date``,
        ``avg_wind_speed_mps``, ``total_net_generation_mwh``.
    """
    _empty = pl.DataFrame(
        {
            "asset_id": pl.Series([], dtype=pl.Utf8),
            "date": pl.Series([], dtype=pl.Utf8),
            "avg_wind_speed_mps": pl.Series([], dtype=pl.Float64),
            "total_net_generation_mwh": pl.Series([], dtype=pl.Float64),
        }
    )

    if daily_df.is_empty():
        return _empty

    wind_ids: list[str] = []
    if not assets_df.is_empty():
        wind_ids = assets_df.filter(pl.col("asset_type").str.to_lowercase() == "wind")[
            "asset_id"
        ].to_list()

    if not wind_ids:
        return _empty

    df = daily_df.filter(pl.col("asset_id").is_in(wind_ids))

    if asset_id != "All":
        df = df.filter(pl.col("asset_id") == asset_id)

    if date_start:
        df = df.filter(pl.col("date") >= date_start)
    if date_end:
        df = df.filter(pl.col("date") <= date_end)

    if df.is_empty():
        return _empty

    return df.select(
        ["asset_id", "date", "avg_wind_speed_mps", "total_net_generation_mwh"]
    ).sort(["asset_id", "date"])


def _prep_solar_scatter(
    daily_df: pl.DataFrame,
    assets_df: pl.DataFrame,
    asset_id: str,
    date_start: str,
    date_end: str,
) -> pl.DataFrame:
    """Filter daily_df to solar assets for the GHI vs generation scatter.

    Parameters
    ----------
    daily_df : pl.DataFrame
        Daily performance rows (asset_id, date, avg_ghi,
        total_net_generation_mwh, ...).
    assets_df : pl.DataFrame
        Asset metadata with asset_id and asset_type columns.
    asset_id : str
        If not ``"All"``, filter to this specific asset (must be a solar asset
        for any rows to be returned).
    date_start : str
        ISO-8601 start date (inclusive). Empty string = no lower bound.
    date_end : str
        ISO-8601 end date (inclusive). Empty string = no upper bound.

    Returns
    -------
    pl.DataFrame
        Rows for solar assets with columns: ``asset_id``, ``date``,
        ``avg_ghi``, ``total_net_generation_mwh``.
    """
    _empty = pl.DataFrame(
        {
            "asset_id": pl.Series([], dtype=pl.Utf8),
            "date": pl.Series([], dtype=pl.Utf8),
            "avg_ghi": pl.Series([], dtype=pl.Float64),
            "total_net_generation_mwh": pl.Series([], dtype=pl.Float64),
        }
    )

    if daily_df.is_empty():
        return _empty

    solar_ids: list[str] = []
    if not assets_df.is_empty():
        solar_ids = assets_df.filter(
            pl.col("asset_type").str.to_lowercase() == "solar"
        )["asset_id"].to_list()

    if not solar_ids:
        return _empty

    df = daily_df.filter(pl.col("asset_id").is_in(solar_ids))

    if asset_id != "All":
        df = df.filter(pl.col("asset_id") == asset_id)

    if date_start:
        df = df.filter(pl.col("date") >= date_start)
    if date_end:
        df = df.filter(pl.col("date") <= date_end)

    if df.is_empty():
        return _empty

    return df.select(["asset_id", "date", "avg_ghi", "total_net_generation_mwh"]).sort(
        ["asset_id", "date"]
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _color_for_index(palette: list[str], i: int) -> str:
    """Return palette[i % len(palette)].

    Parameters
    ----------
    palette : list[str]
        List of hex color strings.
    i : int
        Index into the palette (wraps around).

    Returns
    -------
    str
        Hex color string.
    """
    return palette[i % len(palette)]


# ---------------------------------------------------------------------------
# Chart renderers
# ---------------------------------------------------------------------------


def _render_r2_bars(df: pl.DataFrame) -> Any:
    """Render grouped bar chart of wind/solar R² by asset.

    Parameters
    ----------
    df : pl.DataFrame
        Result of ``_prep_r2_bars`` — one row per asset with
        ``mean_wind_r2`` and ``mean_solar_r2`` columns.

    Returns
    -------
    pn.pane.Bokeh or pn.pane.Markdown
        Bokeh pane for non-empty data, Markdown placeholder otherwise.
    """
    import panel as pn
    from bokeh.models import ColumnDataSource, FactorRange, Legend, LegendItem

    if df.is_empty():
        return pn.pane.Markdown("_No R\u00b2 data available for the current filters._")

    asset_ids: list[str] = df["asset_id"].to_list()
    wind_r2: list[float] = df["mean_wind_r2"].to_list()
    solar_r2: list[float] = df["mean_solar_r2"].to_list()

    x_wind = [(a, "Wind R\u00b2") for a in asset_ids]
    x_solar = [(a, "Solar R\u00b2") for a in asset_ids]
    x_all = x_wind + x_solar
    y_all = wind_r2 + solar_r2
    colors = [_WIND_R2_COLOR] * len(asset_ids) + [_SOLAR_R2_COLOR] * len(asset_ids)

    source = ColumnDataSource({"x": x_all, "y": y_all, "color": colors})

    fig = make_themed_figure(
        "Weather Correlation (R\u00b2) by Asset",
        "Asset",
        "R\u00b2",
        x_range=FactorRange(*x_all),
        height=350,
    )
    style_tooltip(fig, [("Asset", "@x"), ("R\u00b2", "@y{0.000}")])

    wind_bars = fig.vbar(
        x="x",
        top="y",
        width=0.4,
        color="color",
        source=source,
    )

    legend = Legend(
        items=[
            LegendItem(label="Wind R\u00b2", renderers=[wind_bars], index=0),
            LegendItem(
                label="Solar R\u00b2", renderers=[wind_bars], index=len(asset_ids)
            ),
        ]
    )
    fig.add_layout(legend, "right")
    fig.xaxis.major_label_orientation = 0.8

    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


def _render_wind_scatter(df: pl.DataFrame) -> Any:
    """Render wind speed vs generation scatter, coloured by asset.

    Parameters
    ----------
    df : pl.DataFrame
        Result of ``_prep_wind_scatter``.

    Returns
    -------
    pn.pane.Bokeh or pn.pane.Markdown
    """
    import panel as pn

    if df.is_empty():
        return pn.pane.Markdown(
            "_No wind asset data available for the current filters._"
        )

    fig = make_themed_figure(
        "Wind Speed vs. Generation (Wind Assets)",
        "Wind Speed (m/s)",
        "Net Generation (MWh)",
        height=350,
    )
    style_tooltip(
        fig,
        [
            ("Asset", "@asset_id"),
            ("Wind Speed (m/s)", "@x{0.00}"),
            ("MWh", "@y{0.0}"),
        ],
    )

    asset_ids_sorted = sorted(df["asset_id"].unique().to_list())
    for i, aid in enumerate(asset_ids_sorted):
        rows = df.filter(pl.col("asset_id") == aid)
        color = _color_for_index(_WIND_PALETTE, i)
        from bokeh.models import ColumnDataSource

        src = ColumnDataSource(
            {
                "x": rows["avg_wind_speed_mps"].to_list(),
                "y": rows["total_net_generation_mwh"].to_list(),
                "asset_id": [aid] * rows.shape[0],
            }
        )
        fig.scatter(
            x="x",
            y="y",
            source=src,
            fill_color=color,
            line_color=color,
            size=7,
            marker="circle",
            legend_label=aid,
        )

    fig.legend.location = "top_left"
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


def _render_solar_scatter(df: pl.DataFrame) -> Any:
    """Render GHI vs generation scatter, coloured by asset.

    Parameters
    ----------
    df : pl.DataFrame
        Result of ``_prep_solar_scatter``.

    Returns
    -------
    pn.pane.Bokeh or pn.pane.Markdown
    """
    import panel as pn

    if df.is_empty():
        return pn.pane.Markdown(
            "_No solar asset data available for the current filters._"
        )

    fig = make_themed_figure(
        "GHI vs. Generation (Solar Assets)",
        "GHI (W/m\u00b2)",
        "Net Generation (MWh)",
        height=350,
    )
    style_tooltip(
        fig,
        [
            ("Asset", "@asset_id"),
            ("GHI (W/m\u00b2)", "@x{0.00}"),
            ("MWh", "@y{0.0}"),
        ],
    )

    asset_ids_sorted = sorted(df["asset_id"].unique().to_list())
    for i, aid in enumerate(asset_ids_sorted):
        rows = df.filter(pl.col("asset_id") == aid)
        color = _color_for_index(_SOLAR_PALETTE, i)
        from bokeh.models import ColumnDataSource

        src = ColumnDataSource(
            {
                "x": rows["avg_ghi"].to_list(),
                "y": rows["total_net_generation_mwh"].to_list(),
                "asset_id": [aid] * rows.shape[0],
            }
        )
        fig.scatter(
            x="x",
            y="y",
            source=src,
            fill_color=color,
            line_color=color,
            size=7,
            marker="circle",
            legend_label=aid,
        )

    fig.legend.location = "top_left"
    return pn.pane.Bokeh(fig, sizing_mode="stretch_width")


# ---------------------------------------------------------------------------
# Public component
# ---------------------------------------------------------------------------


def weather_panel(filters: Any) -> Any:
    """Build a reactive pn.Column with three weather correlation charts.

    Charts re-render when ``asset_id``, ``asset_type``, ``date_start``, or
    ``date_end`` changes on *filters*.

    The three charts are:
    1. Grouped bar: wind R² and solar R² per asset.
    2. Scatter: wind speed vs. net generation (wind assets only).
    3. Scatter: GHI vs. net generation (solar assets only).

    Parameters
    ----------
    filters : Filters
        Populated ``Filters`` instance with ``_daily_df``, ``_weather_df``,
        and ``_assets_df`` attributes set.

    Returns
    -------
    pn.Column
        Column containing the reactive chart panels.
    """
    import panel as pn

    @pn.depends(
        filters.param.asset_id,
        filters.param.asset_type,
        filters.param.date_start,
        filters.param.date_end,
    )
    def _charts(
        asset_id: str,
        asset_type: str,
        date_start: str,
        date_end: str,
    ) -> Any:
        _raw_daily = getattr(filters, "_daily_df", None)
        daily_df: pl.DataFrame = (
            _raw_daily if _raw_daily is not None else pl.DataFrame()
        )
        _raw_weather = getattr(filters, "_weather_df", None)
        weather_df: pl.DataFrame = (
            _raw_weather if _raw_weather is not None else pl.DataFrame()
        )
        _raw_assets = getattr(filters, "_assets_df", None)
        assets_df: pl.DataFrame = (
            _raw_assets if _raw_assets is not None else pl.DataFrame()
        )

        r2_df = _prep_r2_bars(
            weather_df, assets_df, asset_id, asset_type, date_start, date_end
        )
        wind_df = _prep_wind_scatter(
            daily_df, assets_df, asset_id, date_start, date_end
        )
        solar_df = _prep_solar_scatter(
            daily_df, assets_df, asset_id, date_start, date_end
        )

        chart1 = with_empty_guard(
            r2_df,
            _render_r2_bars,
            message="No R\u00b2 data for the selected filters.",
        )
        chart2 = with_empty_guard(
            wind_df,
            _render_wind_scatter,
            message="No wind asset data for the selected filters.",
        )
        chart3 = with_empty_guard(
            solar_df,
            _render_solar_scatter,
            message="No solar asset data for the selected filters.",
        )

        return pn.Column(chart1, chart2, chart3, sizing_mode="stretch_width")

    return pn.Column(
        pn.panel(_charts),
        sizing_mode="stretch_width",
    )
