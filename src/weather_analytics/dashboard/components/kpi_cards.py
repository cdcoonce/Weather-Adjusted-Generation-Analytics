"""KPI card row for the WAGA dashboard.

Exports:
- ``compute_kpis()`` — pure function (fully testable without Panel)
- ``kpi_row()`` — returns a ``pn.Row`` of four reactive KPI cards

**Bundler note**: this file is inlined by ``scripts/build_dashboard_app.py``
before ``app.py`` is appended. Imports from ``weather_analytics.dashboard.*``
are stripped automatically.
"""

from typing import Any

import polars as pl

_DASH = "—"

# Card CSS applied inline so the bundled Pyodide build is self-contained.
_CARD_CSS = """
.kpi-card {
  background: #ffffff;
  border-radius: 10px;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  padding: 1.25rem 1.5rem;
  font-family: "Poppins", sans-serif;
  min-width: 180px;
  flex: 1;
}
.kpi-value {
  font-size: 2rem;
  font-weight: 600;
  color: #353535;
  margin: 0;
  line-height: 1.1;
}
.kpi-label {
  font-size: 0.7rem;
  font-weight: 500;
  color: #555555;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  margin-top: 0.4rem;
}
"""


def compute_kpis(  # noqa: PLR0913
    daily_df: pl.DataFrame,
    weather_df: pl.DataFrame,
    asset_id: str,
    asset_type: str,
    date_start: str,
    date_end: str,
) -> dict[str, str]:
    """Compute the four dashboard KPI values from filtered DataFrames.

    All filtering happens inside this function so it is pure and fully
    testable without Panel.

    Parameters
    ----------
    daily_df : pl.DataFrame
        Daily performance data. Must contain ``asset_id``, ``asset_type``,
        ``date``, ``total_net_generation_mwh``, ``daily_capacity_factor``,
        and ``avg_availability_pct`` columns.
    weather_df : pl.DataFrame
        Weather-adjusted performance data. Must contain ``asset_id``,
        ``date``, and ``performance_score`` columns.
    asset_id : str
        Selected asset ID or ``"All"`` for no asset filter.
    asset_type : str
        ``"All"``, ``"Wind"``, or ``"Solar"``.
    date_start : str
        ISO-8601 start date (inclusive). Empty string = no lower bound.
    date_end : str
        ISO-8601 end date (inclusive). Empty string = no upper bound.

    Returns
    -------
    dict[str, str]
        Keys: ``total_mwh``, ``avg_capacity_factor``, ``avg_availability``,
        ``avg_performance_score``. Values are formatted strings or ``"—"``
        when the filtered result is empty.
    """
    d = _apply_filters(daily_df, asset_id, asset_type, date_start, date_end)
    w = _apply_filters(weather_df, asset_id, "All", date_start, date_end)

    total_mwh = _safe_sum(d, "total_net_generation_mwh")
    avg_cf = _safe_mean(d, "daily_capacity_factor", decimals=4)
    avg_avail = _safe_mean(d, "avg_availability_pct", decimals=1)
    avg_perf = _safe_mean(w, "performance_score", decimals=4)

    return {
        "total_mwh": total_mwh,
        "avg_capacity_factor": avg_cf,
        "avg_availability": avg_avail,
        "avg_performance_score": avg_perf,
    }


def _apply_filters(
    df: pl.DataFrame,
    asset_id: str,
    asset_type: str,
    date_start: str,
    date_end: str,
) -> pl.DataFrame:
    """Return *df* narrowed to rows matching all active filters."""
    result = df
    if asset_id != "All" and "asset_id" in result.columns:
        result = result.filter(pl.col("asset_id") == asset_id)
    if asset_type != "All" and "asset_type" in result.columns:
        result = result.filter(pl.col("asset_type") == asset_type)
    if date_start and "date" in result.columns:
        result = result.filter(pl.col("date") >= date_start)
    if date_end and "date" in result.columns:
        result = result.filter(pl.col("date") <= date_end)
    return result


def _safe_sum(df: pl.DataFrame, col: str) -> str:
    """Return sum of *col* as a string, or ``"—"`` if empty."""
    if df.is_empty() or col not in df.columns:
        return _DASH
    total = df[col].sum()
    if total is None:
        return _DASH
    return str(float(total))


def _safe_mean(df: pl.DataFrame, col: str, *, decimals: int = 2) -> str:
    """Return mean of *col* rounded to *decimals* places, or ``"—"`` if empty."""
    if df.is_empty() or col not in df.columns:
        return _DASH
    mean_val = df[col].mean()
    if mean_val is None:
        return _DASH
    return f"{float(mean_val):.{decimals}f}"


def kpi_row(filters: Any) -> Any:
    """Return a ``pn.Row`` of four reactive KPI cards driven by *filters*.

    Each card re-renders whenever any filter param changes. Data is read
    from ``filters._daily_df`` and ``filters._weather_df`` which are
    populated by ``app.py`` after the loaders resolve.

    Parameters
    ----------
    filters : Filters
        Populated ``Filters`` instance (after ``initialize()`` has been
        called and ``_daily_df`` / ``_weather_df`` have been set).

    Returns
    -------
    pn.Row
        A Panel row containing four ``pn.pane.HTML`` KPI cards.
    """
    import panel as pn

    def _card(label: str, value_key: str) -> Any:
        @pn.depends(
            filters.param.asset_id,
            filters.param.asset_type,
            filters.param.date_start,
            filters.param.date_end,
        )
        def _render(
            asset_id: str,
            asset_type: str,
            date_start: str,
            date_end: str,
        ) -> pn.pane.HTML:
            daily_df: pl.DataFrame = (
                getattr(filters, "_daily_df", None) or pl.DataFrame()
            )
            weather_df: pl.DataFrame = (
                getattr(filters, "_weather_df", None) or pl.DataFrame()
            )
            kpis = compute_kpis(
                daily_df, weather_df, asset_id, asset_type, date_start, date_end
            )
            value = kpis[value_key]
            html = (
                f'<div class="kpi-card">'
                f'<p class="kpi-value">{value}</p>'
                f'<p class="kpi-label">{label}</p>'
                f"</div>"
            )
            return pn.pane.HTML(html, sizing_mode="stretch_width")

        return pn.panel(_render)

    return pn.Row(
        _card("Total MWh", "total_mwh"),
        _card("Avg Capacity Factor", "avg_capacity_factor"),
        _card("Avg Availability %", "avg_availability"),
        _card("Avg Performance Score", "avg_performance_score"),
        sizing_mode="stretch_width",
        stylesheets=[_CARD_CSS],
    )
