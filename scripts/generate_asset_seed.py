"""Generate the dbt ``asset_dimension`` seed from the fleet registry.

The fleet (:data:`weather_analytics.mock_data.fleet.FLEET`) is the single source
of truth for asset metadata (name, technology, capacity, coordinates, region).
This script materializes it as a dbt seed CSV so the Snowflake warehouse has a
queryable asset dimension — ``WAGA.SEEDS.asset_dimension`` after ``dbt seed`` —
that the marts and dashboard export join for real site names and locations.

Regenerate whenever ``fleet.FLEET`` changes::

    uv run python scripts/generate_asset_seed.py
"""

from __future__ import annotations

import csv
from pathlib import Path

from weather_analytics.mock_data.fleet import FLEET

_SEED_PATH = (
    Path(__file__).resolve().parents[1]
    / "dbt"
    / "renewable_dbt"
    / "seeds"
    / "asset_dimension.csv"
)

_COLUMNS = (
    "asset_id",
    "asset_name",
    "asset_type",
    "capacity_mw",
    "latitude",
    "longitude",
    "region",
)


def write_seed(path: Path = _SEED_PATH) -> Path:
    """Write the asset-dimension seed CSV from ``FLEET``; return the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(_COLUMNS)
        for asset in FLEET:
            writer.writerow(
                [
                    asset.asset_id,
                    asset.name,
                    asset.asset_type,
                    f"{asset.capacity_mw:g}",
                    f"{asset.latitude:g}",
                    f"{asset.longitude:g}",
                    asset.region,
                ]
            )
    return path


if __name__ == "__main__":
    written = write_seed()
    print(f"wrote {written} ({len(FLEET)} assets)")
