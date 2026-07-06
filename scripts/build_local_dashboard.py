"""Regenerate the WAGA dashboard from a local fleet simulation — no Snowflake.

Runs the multi-technology fleet simulation (wind, solar, battery, gas), writes
the four ``dashboard_exports/*.json`` files, and (optionally) renders the static
``dist/index.html`` the cockpit deploys. Real hourly weather is pulled from the
free Open-Meteo archive API by default, falling back to a physical synthetic
model when offline.

Examples
--------
Regenerate a year of data and build the page::

    python scripts/build_local_dashboard.py --start 2025-07-01 --end 2026-06-30 --build

Deterministic offline run (CI)::

    python scripts/build_local_dashboard.py --start 2026-01-01 --end 2026-03-31 \\
        --synthetic --build
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from weather_analytics.cockpit.config import DEFAULT_EXPORT_DIR, DEFAULT_OUT
from weather_analytics.cockpit.data import load_dataset
from weather_analytics.cockpit.render import render_dashboard
from weather_analytics.mock_data.local_export import build_local_exports


def main(argv: list[str] | None = None) -> int:
    """Parse args, run the simulation, write exports, optionally render."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2025-07-01", help="start date (ISO)")
    parser.add_argument("--end", default="2026-06-30", help="end date (ISO)")
    parser.add_argument("--out-dir", default=DEFAULT_EXPORT_DIR)
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="force synthetic weather (skip the Open-Meteo pull)",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--build",
        action="store_true",
        help="also render the static dashboard to dist/index.html",
    )
    parser.add_argument("--out", default=DEFAULT_OUT, help="rendered HTML path")
    args = parser.parse_args(argv)

    start = f"{args.start}T00:00:00" if "T" not in args.start else args.start
    end = f"{args.end}T23:00:00" if "T" not in args.end else args.end

    print(f"Simulating fleet {start} -> {end} (seed={args.seed}) ...")
    manifest = build_local_exports(
        start_date=start,
        end_date=end,
        out_dir=Path(args.out_dir),
        use_real_weather=not args.synthetic,
        random_seed=args.seed,
    )
    print(
        f"  weather source : {manifest['weather_source']}\n"
        f"  assets         : {manifest['asset_count']} "
        f"{manifest['asset_type_counts']}\n"
        f"  daily rows     : {manifest['row_counts']['daily_performance']}\n"
        f"  wrote exports  : {args.out_dir}"
    )

    if args.build:
        dataset = load_dataset(Path(args.out_dir))
        render_dashboard(dataset, Path(args.out))
        print(f"  built dashboard: {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
