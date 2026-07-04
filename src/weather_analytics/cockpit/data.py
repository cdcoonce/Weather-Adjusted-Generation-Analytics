"""Load the 4 JSON exports into typed structures.

Pure stdlib. Unknown extra keys in the JSON are ignored; the loaders pull only
the fields the dashboard uses, but ``Dataset.raw`` keeps the full parsed
payloads for the client-side JSON island.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Asset:
    asset_id: str
    capacity_mw: float
    size_category: str
    asset_type: str
    display_name: str


@dataclass(frozen=True)
class DailyRow:
    asset_id: str
    date: str
    total_net_generation_mwh: float
    daily_capacity_factor: float
    avg_availability_pct: float
    total_curtailment_mwh: float
    daily_performance_rating: str


@dataclass(frozen=True)
class WeatherRow:
    asset_id: str
    date: str
    performance_score: float
    performance_category: str
    inferred_asset_type: str


@dataclass(frozen=True)
class Manifest:
    generated_at: str
    date_range_start: str
    date_range_end: str
    asset_count: int
    schema_version: str


@dataclass(frozen=True)
class Dataset:
    manifest: Manifest
    assets: list[Asset]
    daily: list[DailyRow]
    weather: list[WeatherRow]
    raw: dict


def _num(value: object, default: float = 0.0) -> float:
    """Coerce a JSON value to float, mapping null/None/missing to ``default``."""
    if value is None:
        return default
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def load_dataset(export_dir: Path) -> Dataset:
    """Read manifest/assets/daily_performance/weather_performance from export_dir."""
    export_dir = Path(export_dir)
    manifest_raw = _load_json(export_dir / "manifest.json")
    assets_raw = _load_json(export_dir / "assets.json")
    daily_raw = _load_json(export_dir / "daily_performance.json")
    weather_raw = _load_json(export_dir / "weather_performance.json")

    date_range = manifest_raw.get("date_range", {})
    manifest = Manifest(
        generated_at=str(manifest_raw.get("generated_at", "")),
        date_range_start=str(date_range.get("start", "")),
        date_range_end=str(date_range.get("end", "")),
        asset_count=int(manifest_raw.get("asset_count", 0)),
        schema_version=str(manifest_raw.get("schema_version", "")),
    )

    assets = [
        Asset(
            asset_id=str(a["asset_id"]),
            capacity_mw=_num(a.get("capacity_mw")),
            size_category=str(a.get("size_category", "")),
            asset_type=str(a.get("asset_type", "")),
            display_name=str(a.get("display_name", a.get("asset_id", ""))),
        )
        for a in assets_raw
    ]

    daily = [
        DailyRow(
            asset_id=str(r["asset_id"]),
            date=str(r["date"]),
            total_net_generation_mwh=_num(r.get("total_net_generation_mwh")),
            daily_capacity_factor=_num(r.get("daily_capacity_factor")),
            avg_availability_pct=_num(r.get("avg_availability_pct")),
            total_curtailment_mwh=_num(r.get("total_curtailment_mwh")),
            daily_performance_rating=str(r.get("daily_performance_rating", "")),
        )
        for r in daily_raw
    ]

    weather = [
        WeatherRow(
            asset_id=str(r["asset_id"]),
            date=str(r["date"]),
            performance_score=_num(r.get("performance_score")),
            performance_category=str(r.get("performance_category", "")),
            inferred_asset_type=str(r.get("inferred_asset_type", "")),
        )
        for r in weather_raw
    ]

    raw = {
        "manifest": manifest_raw,
        "assets": assets_raw,
        "daily": daily_raw,
        "weather": weather_raw,
    }
    return Dataset(
        manifest=manifest, assets=assets, daily=daily, weather=weather, raw=raw
    )
