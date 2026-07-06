"""Pure KPI + inline-SVG geometry. No chart library.

Every function takes the typed Dataset (plus optional asset/date filters) and
returns plain dicts/lists/strings — never a DataFrame. The client-side app.js
mirrors this math to redraw on filter changes; keep the two in sync.

The fleet is mixed-technology (wind, solar, battery, gas); helpers here are
type-aware — batteries are excluded from fleet capacity factor (their net energy
is negative over a cycle) and each technology gets a stable series color.
"""

from __future__ import annotations

from statistics import fmean

from weather_analytics.cockpit.data import DailyRow, Dataset, WeatherRow

# Technology series colors — echo the portfolio's data-viz palette (green/gold)
# and extend it with distinct, light-theme-friendly hues for storage and gas.
TYPE_COLORS: dict[str, str] = {
    "wind": "#2f6f5f",
    "solar": "#d4a12e",
    "battery": "#3f7cac",
    "gas": "#9c5b3b",
    "unknown": "#8a8f98",
}

_RENEWABLE = ("wind", "solar")
_CF_TYPES = ("wind", "solar", "gas")  # battery CF is not meaningful
_PRIMARY = "#2f6f5f"  # portfolio data-viz green
_SECONDARY = "#b0872f"  # portfolio data-viz gold


def type_color(asset_type: str) -> str:
    """Series color for a technology (falls back to a neutral gray)."""
    return TYPE_COLORS.get(asset_type, TYPE_COLORS["unknown"])


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


def _type_by_id(dataset: Dataset) -> dict[str, str]:
    return {a.asset_id: a.asset_type for a in dataset.assets}


def fleet_kpis(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    """Six headline fleet metrics for the KPI strip."""
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    type_of = _type_by_id(dataset)

    def _type(r: DailyRow) -> str:
        return r.asset_type or type_of.get(r.asset_id, "")

    net_gen = sum(r.total_net_generation_mwh for r in daily)
    curtailment = sum(r.total_curtailment_mwh for r in daily)
    co2 = sum(r.total_co2_tonnes for r in daily)
    battery_throughput = sum(
        r.total_discharge_mwh for r in daily if _type(r) == "battery"
    )
    renewable_gen = sum(
        r.total_net_generation_mwh for r in daily if _type(r) in _RENEWABLE
    )
    served = sum(r.total_net_generation_mwh for r in daily if _type(r) in _CF_TYPES)
    cf_rows = [r.daily_capacity_factor for r in daily if _type(r) in _CF_TYPES]
    cf = fmean(cf_rows) if cf_rows else 0.0
    renewable_share = (renewable_gen / served * 100.0) if served > 0 else 0.0

    return [
        {
            "key": "net_generation",
            "label": "net generation",
            "value": f"{net_gen:,.0f}",
            "unit": "MWh",
        },
        {
            "key": "capacity_factor",
            "label": "fleet capacity factor",
            "value": f"{cf * 100:.1f}%",
            "unit": "wind · solar · gas",
        },
        {
            "key": "renewable_share",
            "label": "renewable share",
            "value": f"{renewable_share:.0f}%",
            "unit": "of served load",
        },
        {
            "key": "co2_emissions",
            "label": "CO₂ emissions",
            "value": f"{co2:,.0f}",
            "unit": "tonnes",
        },
        {
            "key": "battery_throughput",
            "label": "battery discharge",
            "value": f"{battery_throughput:,.0f}",
            "unit": "MWh delivered",
        },
        {
            "key": "curtailment",
            "label": "curtailment",
            "value": f"{curtailment:,.0f}",
            "unit": "MWh",
        },
    ]


def line_series(
    pairs: list[tuple[str, float]],
    width: int = 720,
    height: int = 200,
    pad: int = 8,
    color: str = "var(--accent)",
) -> dict | None:
    """Turn ordered (date, value) pairs into SVG polyline + filled-area geometry."""
    if not pairs:
        return None
    values = [v for _, v in pairs]
    y_min = min([0.0, *values])
    y_max = max(values)
    span = (y_max - y_min) or 1.0
    n = len(pairs)

    def x(i: int) -> float:
        return pad + (i / (n - 1) * (width - 2 * pad) if n > 1 else 0.0)

    def y(v: float) -> float:
        return height - pad - ((v - y_min) / span) * (height - 2 * pad)

    baseline = y(0.0)
    pts = [(x(i), y(v)) for i, v in enumerate(values)]
    polyline = " ".join(f"{px:.1f},{py:.1f}" for px, py in pts)
    area = (
        f"M {pts[0][0]:.1f} {baseline:.1f} "
        + " ".join(f"L {px:.1f} {py:.1f}" for px, py in pts)
        + f" L {pts[-1][0]:.1f} {baseline:.1f} Z"
    )
    return {
        "width": width,
        "height": height,
        "pad": pad,
        "area_path": area,
        "polyline": polyline,
        "color": color,
        "y_max": y_max,
        "x0_label": pairs[0][0],
        "x1_label": pairs[-1][0],
    }


def _by_date_sum(
    rows: list[DailyRow],
    attr: str,
    types: tuple[str, ...] | None = None,
    type_of: dict[str, str] | None = None,
) -> list[tuple[str, float]]:
    acc: dict[str, float] = {}
    for r in rows:
        if types is not None:
            t = r.asset_type or (type_of or {}).get(r.asset_id, "")
            if t not in types:
                continue
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
    return line_series(_by_date_sum(daily, "total_net_generation_mwh"), color=_PRIMARY)


def capacity_factor_series(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict | None:
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    type_of = _type_by_id(dataset)
    cf_rows = [
        (r.date, r.daily_capacity_factor)
        for r in daily
        if (r.asset_type or type_of.get(r.asset_id, "")) in _CF_TYPES
    ]
    return line_series(_by_date_mean(cf_rows), height=160, color=_PRIMARY)


def performance_series(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict | None:
    weather = filter_weather(dataset.weather, asset_ids, start, end)
    pairs = _by_date_mean([(r.date, r.performance_score) for r in weather])
    return line_series(pairs, height=160, color=_SECONDARY)


def battery_soc_series(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict | None:
    """Mean battery state-of-charge (%) over time."""
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    type_of = _type_by_id(dataset)
    pairs = _by_date_mean(
        [
            (r.date, r.avg_soc_pct)
            for r in daily
            if (r.asset_type or type_of.get(r.asset_id, "")) == "battery"
            and r.avg_soc_pct is not None
        ]
    )
    return line_series(pairs, height=160, color=TYPE_COLORS["battery"])


def emissions_series(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict | None:
    """Total gas CO₂ emissions (tonnes) over time."""
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    type_of = _type_by_id(dataset)
    pairs = _by_date_sum(daily, "total_co2_tonnes", ("gas",), type_of)
    return line_series(pairs, height=160, color=TYPE_COLORS["gas"])


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
    type_by_id = _type_by_id(dataset)
    name_by_id = {a.asset_id: a.display_name for a in dataset.assets}
    # Batteries have near-zero/negative CF; show them by throughput utilization.
    groups: dict[str, list[float]] = {}
    for r in daily:
        groups.setdefault(r.asset_id, []).append(r.daily_capacity_factor)
    means = sorted((aid, fmean(vs)) for aid, vs in groups.items())
    extra = {
        aid: {
            "asset_type": type_by_id.get(aid, ""),
            "color": type_color(type_by_id.get(aid, "")),
            "name": name_by_id.get(aid, aid),
        }
        for aid, _ in means
    }
    return _hbars(means, extra)


def type_split(
    dataset: Dataset,
    asset_ids: set[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    daily = filter_daily(dataset.daily, asset_ids, start, end)
    type_by_id = _type_by_id(dataset)
    totals: dict[str, float] = {}
    for r in daily:
        t = r.asset_type or type_by_id.get(r.asset_id, "unknown")
        totals[t] = totals.get(t, 0.0) + r.total_net_generation_mwh
    extra = {t: {"color": type_color(t)} for t in totals}
    return _hbars(sorted(totals.items()), extra)
