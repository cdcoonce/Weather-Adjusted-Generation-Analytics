#!/usr/bin/env python3
"""Generate and (optionally) install macOS launchd agents for WAGA schedules.

Dagster Cloud (dagster.plus) was retired; the pipeline's schedules now run
locally under ``launchd``. This script generates a ``launchd`` property list
for each scheduled job and, on ``install --load``, drops it into
``~/Library/LaunchAgents`` and loads it.

Plists are generated in Python via :mod:`plistlib` — there is no separate
template file. Each agent runs::

    <python> scripts/run_scheduled.py <job>

at the local time given in :data:`SCHEDULES`.

Subcommands
-----------
- ``install [--load]`` — write plists; with ``--load`` also (re)load them.
- ``uninstall`` — unload and remove the plists.
- ``dry-run`` — print the generated plist XML to stdout without writing.

This script NEVER loads an agent unless ``--load`` is passed explicitly.
"""

from __future__ import annotations

import argparse
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
LABEL_PREFIX = "com.charleslikesdata.waga"
RUNNER = REPO / "scripts" / "run_scheduled.py"
LOGS_DIR = REPO / "logs"

# Job -> StartCalendarInterval (LOCAL time). launchd interprets these keys in
# the machine's local timezone.
#   daily  — every day at 06:00
#   weekly — Mondays (Weekday 1) at 06:30
SCHEDULES: dict[str, dict[str, int]] = {
    "daily": {"Hour": 6, "Minute": 0},
    "weekly": {"Weekday": 1, "Hour": 6, "Minute": 30},
}


def _uv_dir() -> str:
    """Directory containing the ``uv`` binary, for the agent's PATH."""
    uv_path = shutil.which("uv")
    if uv_path:
        return str(Path(uv_path).parent)
    return str(Path.home() / ".local" / "bin")


def _launch_python() -> str:
    """Stable interpreter for the launchd agent's ProgramArguments[0].

    ``run_scheduled.py`` is stdlib-only, so prefer the system ``python3`` —
    always present on macOS and unaffected by a rebuilt/relocated project venv.
    Falls back to the current interpreter only if the system one is absent.
    """
    system_python = Path("/usr/bin/python3")
    if system_python.exists():
        return str(system_python)
    return sys.executable


def _label(job: str) -> str:
    return f"{LABEL_PREFIX}.{job}"


def _plist_path(job: str) -> Path:
    return LAUNCH_AGENTS_DIR / f"{_label(job)}.plist"


def build_plist(job: str, interval: dict[str, int]) -> dict[str, object]:
    """Build the plist dictionary for a single scheduled job."""
    path_env = f"{_uv_dir()}:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    label = _label(job)
    return {
        "Label": label,
        "ProgramArguments": [_launch_python(), str(RUNNER), job],
        "WorkingDirectory": str(REPO),
        "StartCalendarInterval": interval,
        "EnvironmentVariables": {"PATH": path_env},
        "StandardOutPath": str(LOGS_DIR / f"{label}.out.log"),
        "StandardErrorPath": str(LOGS_DIR / f"{label}.err.log"),
        "RunAtLoad": False,
        "ProcessType": "Background",
    }


def _cmd_install(load: bool) -> int:
    LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    for job, interval in SCHEDULES.items():
        plist = build_plist(job, interval)
        target = _plist_path(job)
        with target.open("wb") as handle:
            plistlib.dump(plist, handle)
        print(f"Wrote {target}")
        if load:
            # Unload any prior copy first (ignore failure — may not be loaded),
            # then load. Only reached when --load is explicitly requested.
            subprocess.run(
                ["launchctl", "unload", str(target)],
                check=False,
            )
            subprocess.run(
                ["launchctl", "load", str(target)],
                check=True,
            )
            print(f"Loaded {_label(job)}")
    if not load:
        print(
            "Plists written but NOT loaded. Re-run with --load, or "
            "`launchctl load <plist>` each one manually.",
        )
    return 0


def _cmd_uninstall() -> int:
    for job in SCHEDULES:
        target = _plist_path(job)
        subprocess.run(
            ["launchctl", "unload", str(target)],
            check=False,
        )
        if target.exists():
            target.unlink()
            print(f"Removed {target}")
        else:
            print(f"Not present: {target}")
    return 0


def _cmd_dry_run() -> int:
    for job, interval in SCHEDULES.items():
        plist = build_plist(job, interval)
        print(f"# ---- {_plist_path(job)} ----")
        print(plistlib.dumps(plist).decode())
    return 0


def main() -> int:
    """Parse args and dispatch to the requested subcommand."""
    if sys.platform != "darwin":
        print(
            "install_launchd.py only supports macOS (launchd). "
            f"Detected platform: {sys.platform}",
            file=sys.stderr,
        )
        return 1

    parser = argparse.ArgumentParser(
        description="Generate/install launchd agents for WAGA schedules.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    install = sub.add_parser("install", help="write plists (optionally load)")
    install.add_argument(
        "--load",
        action="store_true",
        help="also launchctl load each agent (never done without this flag)",
    )
    sub.add_parser("uninstall", help="unload and remove plists")
    sub.add_parser("dry-run", help="print generated plists without writing")

    args = parser.parse_args()

    if args.command == "install":
        return _cmd_install(load=args.load)
    if args.command == "uninstall":
        return _cmd_uninstall()
    if args.command == "dry-run":
        return _cmd_dry_run()
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
