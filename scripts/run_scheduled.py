#!/usr/bin/env python3
"""launchd entrypoint for local, scheduled Dagster materializations.

Dagster Cloud (dagster.plus) was retired, so the pipeline's schedules now run
locally via macOS ``launchd``. Each launchd agent invokes this script with a
single job name; the script fans that job out into an ordered chain of
``uv run dagster asset materialize`` steps against yesterday's partition.

Deliberately dependency-free (stdlib only) so launchd can run it with the
system ``python3`` without a virtualenv — the actual Dagster invocation goes
through ``uv run`` so it uses the project's locked environment.

Usage
-----
    python scripts/run_scheduled.py <job>

where ``<job>`` is one of the keys in :data:`JOBS`.
"""

from __future__ import annotations

import argparse
import datetime
import os
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MODULE = "weather_analytics.definitions"


def load_dotenv(env_path: Path) -> None:
    """Load ``KEY=VALUE`` pairs from ``env_path`` into ``os.environ``.

    Minimal, dependency-free parser. Existing environment values win
    (``setdefault``) so launchd / user overrides take precedence over the
    committed ``.env``. Blank lines, ``#`` comments, and lines without ``=``
    are skipped. A leading ``export `` is stripped, and surrounding single or
    double quotes are removed from the value.
    """
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        if key:
            os.environ.setdefault(key, value)


# Yesterday's partition, computed in Python. macOS ``date`` is BSD (different
# flags from GNU), so we never shell out to it. Computed in UTC to match the
# assets' DailyPartitionsDefinition (UTC) and the retired schedules'
# execution_timezone="UTC" — so "yesterday" is always a valid partition key
# regardless of the machine's local timezone or run hour.
# NB: use datetime.timezone.utc (not datetime.UTC) — this stdlib-only script
# runs under the macOS system python3 (3.9), which predates the datetime.UTC
# alias (3.11+). ruff's UP017 assumes a newer interpreter, so it's suppressed.
_YESTERDAY = (
    datetime.datetime.now(datetime.timezone.utc).date()  # noqa: UP017
    - timedelta(days=1)
).strftime("%Y-%m-%d")

# Resolve uv. launchd runs with a minimal PATH, so fall back to the standard
# per-user install location if ``which`` comes up empty.
UV = shutil.which("uv") or str(Path.home() / ".local/bin/uv")

# Job -> ordered list of ``dagster`` CLI argv lists. ``{partition}``
# placeholders are formatted with yesterday's date. Each argv is run as
# ``uv run dagster <argv...>`` from the repo root, in order; a non-zero exit
# aborts the remainder of the chain.
#
# NOTE: the daily chain ends by (1) materializing waga_dashboard_export_build,
# which writes the 4 JSON exports to dashboard_exports/, then (2) running the
# cockpit build + deploy POST_STEPS below to render and publish the static
# dashboard to Cloudflare Pages. The old waga_dashboard_export_publish asset
# (push to a stale portfolio 'master') was removed with the Pyodide dashboard.
JOBS: dict[str, list[list[str]]] = {
    "daily": [
        [
            "asset",
            "materialize",
            "--select",
            "waga_weather_ingestion,waga_generation_ingestion",
            "--partition",
            "{partition}",
            "-m",
            MODULE,
        ],
        [
            "asset",
            "materialize",
            "--select",
            "group:default",
            "-m",
            MODULE,
        ],
        [
            "asset",
            "materialize",
            "--select",
            "waga_dashboard_export_build",
            "-m",
            MODULE,
        ],
    ],
    "weekly": [
        [
            "asset",
            "materialize",
            "--select",
            "waga_correlation_analysis",
            "-m",
            MODULE,
        ],
    ],
}

# Post-Dagster steps: full argv lists run verbatim (NOT wrapped in dagster).
# For `daily`, render the static dashboard from the fresh JSON exports and
# deploy it to Cloudflare Pages. wrangler reads CLOUDFLARE_* from the env that
# load_dotenv() populated above.
POST_STEPS: dict[str, list[list[str]]] = {
    "daily": [
        [UV, "run", "python", "-m", "weather_analytics.cockpit", "build"],
        [UV, "run", "python", "-m", "weather_analytics.cockpit", "deploy"],
    ],
}


def main() -> int:
    """Run the requested job's step chain, logging to file and stdout."""
    parser = argparse.ArgumentParser(
        description="Run a scheduled Dagster job chain (launchd entrypoint).",
    )
    parser.add_argument("job", choices=sorted(JOBS))
    args = parser.parse_args()

    load_dotenv(REPO / ".env")

    # The committed .env may carry a stale DAGSTER_HOME from the Dagster Cloud
    # era. Force it to the repo-local instance store unconditionally (assign,
    # not setdefault) so local runs never point at a phantom home.
    os.environ["DAGSTER_HOME"] = str(REPO / ".dagster")
    (REPO / ".dagster").mkdir(parents=True, exist_ok=True)
    logs_dir = REPO / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    steps = [
        [part.format(partition=_YESTERDAY) for part in step]
        for step in JOBS[args.job]
    ]

    # Local wall-clock timestamp for the log filename is intentional (DTZ005).
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")  # noqa: DTZ005
    log_path = logs_dir / f"scheduled-{args.job}-{timestamp}.log"

    with log_path.open("w", buffering=1) as log:

        def emit(message: str) -> None:
            print(message)
            log.write(message + "\n")
            log.flush()

        emit(f"=== scheduled run: job={args.job} partition={_YESTERDAY} ===")
        emit(f"repo={REPO}")
        emit(f"uv={UV}")
        emit(f"dagster_home={os.environ['DAGSTER_HOME']}")

        for index, step in enumerate(steps, start=1):
            # Invoke via ``python -m dagster`` (not the ``dagster`` console
            # script) so a stale entry-point shebang from a relocated/rebuilt
            # venv can't break the unattended run.
            cmd = [UV, "run", "python", "-m", "dagster", *step]
            emit(f"--- step {index}/{len(steps)}: {' '.join(cmd)} ---")
            result = subprocess.run(  # noqa: PLW1510 - returncode handled below
                cmd,
                cwd=REPO,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            log.flush()
            if result.returncode != 0:
                emit(f"FAILED (exit {result.returncode}) on step {index}")
                return result.returncode

        emit("=== all steps succeeded ===")

        post_steps = POST_STEPS.get(args.job, [])
        for index, cmd in enumerate(post_steps, start=1):
            emit(f"--- post-step {index}/{len(post_steps)}: {' '.join(cmd)} ---")
            result = subprocess.run(  # noqa: PLW1510 - returncode handled below
                cmd, cwd=REPO, stdout=log, stderr=subprocess.STDOUT
            )
            log.flush()
            if result.returncode != 0:
                emit(f"POST-STEP FAILED (exit {result.returncode}) on step {index}")
                return result.returncode
        emit("=== all post-steps succeeded ===")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
