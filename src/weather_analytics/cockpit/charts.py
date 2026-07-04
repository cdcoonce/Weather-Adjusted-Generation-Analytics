"""Pure KPI + inline-SVG geometry. No chart library.

Every function takes the typed Dataset (plus optional asset/date filters) and
returns plain dicts/lists/strings — never a DataFrame. The client-side app.js
mirrors this math to redraw on filter changes; keep the two in sync.
"""

from __future__ import annotations

from statistics import fmean

from weather_analytics.cockpit.data import DailyRow, Dataset, WeatherRow


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
        {
            "key": "capacity_factor",
            "label": "fleet capacity factor",
            "value": f"{cf * 100:.1f}%",
        },
        {
            "key": "net_generation",
            "label": "net generation (MWh)",
            "value": f"{net_gen:,.0f}",
        },
        {
            "key": "performance_score",
            "label": "avg weather-adj. score",
            "value": f"{perf:.2f}",
        },
        {
            "key": "curtailment",
            "label": "curtailment (MWh)",
            "value": f"{curtailment:,.0f}",
        },
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
