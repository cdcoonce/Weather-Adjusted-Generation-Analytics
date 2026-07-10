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
import socket
import subprocess
import time
import urllib.request
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import TextIO

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


def wait_for_network(  # noqa: PLR0913 - injectable probe/clock for tests
    host: str = "pypi.org",
    port: int = 443,
    timeout_s: float = 300.0,
    interval_s: float = 15.0,
    probe: Callable[[str, int], object] = socket.getaddrinfo,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    emit: Callable[[str], None] = print,
) -> bool:
    """Wait until DNS resolves ``host``, up to ``timeout_s`` seconds.

    launchd fires a missed calendar job immediately on wake, which can be
    before the network/DNS is up (observed 2026-07-10: `uv run` needed PyPI to
    rebuild the project after a merge and aborted the chain on a DNS error).
    Probing DNS instead of opening a connection keeps this fast and free of
    remote side effects.

    Returns True once the probe succeeds, False on timeout. A False return
    does NOT abort the run — with an unchanged tree the chain works offline,
    and steps that do need the network fail loudly and get retried.
    """
    deadline = monotonic() + timeout_s
    attempt = 1
    while True:
        try:
            probe(host, port)
        except OSError as exc:
            remaining = deadline - monotonic()
            if remaining <= 0:
                emit(
                    f"network: {host} still unresolvable after {timeout_s:.0f}s "
                    "- proceeding anyway (offline runs work when the tree is "
                    "unchanged)"
                )
                return False
            emit(
                f"network: attempt {attempt} failed ({exc}); "
                f"retrying in {interval_s:.0f}s"
            )
            sleep(min(interval_s, remaining))
            attempt += 1
        else:
            if attempt > 1:
                emit(f"network: reachable after {attempt} attempts")
            return True


def run_step_with_retries(  # noqa: PLR0913 - injectable runner/sleep for tests
    cmd: list[str],
    *,
    label: str,
    emit: Callable[[str], None],
    log_file: TextIO,
    attempts: int = 2,
    retry_delay_s: float = 60.0,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Run ``cmd``, retrying up to ``attempts`` total tries on non-zero exit.

    Safe because every step in the chain is idempotent: dlt merges on
    ``(asset_id, timestamp)``, dbt builds are re-runnable, and the cockpit
    build/deploy just re-renders and re-uploads. Covers transient failures
    (Snowflake blips, network hiccups) that would otherwise silently skip a
    partition — nothing alerts on a failed unattended run.

    Returns the last exit code (0 on success).
    """
    returncode = 1
    for attempt in range(1, attempts + 1):
        result = runner(cmd, cwd=REPO, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.flush()
        returncode = result.returncode
        if returncode == 0:
            return 0
        if attempt < attempts:
            emit(
                f"{label}: exit {returncode} (attempt {attempt}/{attempts}); "
                f"retrying in {retry_delay_s:.0f}s"
            )
            sleep(retry_delay_s)
    return returncode


def hold_wake_assertion(
    pid: int,
    emit: Callable[[str], None] = print,
    popen: Callable[..., object] = subprocess.Popen,
) -> object | None:
    """Keep the machine awake while ``pid`` is alive (caffeinate sidecar).

    launchd fires the 06:00 job during a DarkWake window; without a power
    assertion the machine goes back to sleep ~3 minutes later and the run is
    suspended, resuming in ~3-minute slices every ~16 minutes (observed
    2026-07-09: a ~10-minute chain took 3.5 h wall-clock). ``caffeinate -w``
    holds the assertion until this process exits, then releases it — no
    cleanup needed. ``-i`` prevents idle sleep; ``-s`` additionally holds the
    system awake on AC power.

    Returns the sidecar process, or None when caffeinate can't be spawned
    (non-macOS or stripped environment) — the run proceeds without it.
    """
    try:
        return popen(["/usr/bin/caffeinate", "-i", "-s", "-w", str(pid)])
    except OSError as exc:
        emit(f"caffeinate unavailable ({exc}); continuing without wake assertion")
        return None


def report_outcome(  # noqa: PLR0913 - injectable runner/urlopen for tests
    job: str,
    returncode: int,
    log_path: Path,
    emit: Callable[[str], None] = print,
    runner: Callable[..., object] = subprocess.run,
    urlopen: Callable[..., object] = urllib.request.urlopen,
) -> None:
    """Signal the run's outcome beyond the log file — nothing else alerts.

    - On failure, post a macOS notification (visible at next login/glance).
    - When ``WAGA_HEALTHCHECK_URL`` is set (via .env or the environment),
      ping it healthchecks-style: GET <url> on success, GET <url>/fail on
      failure. Because a machine that never ran pings nothing, a missing
      success ping is itself the alert — a dead-man's switch that also
      catches launchd never firing at all.

    Never raises, and never alters the run's exit code.
    """
    if returncode != 0:
        message = f"{job} run failed (exit {returncode}) - see {log_path.name}"
        script = 'display notification "{}" with title "WAGA scheduled run"'.format(
            message.replace('"', "'")
        )
        try:
            runner(["/usr/bin/osascript", "-e", script], check=False)
        except OSError as exc:
            emit(f"notification failed: {exc}")

    url = os.environ.get("WAGA_HEALTHCHECK_URL", "").strip()
    if not url:
        return
    target = url if returncode == 0 else url.rstrip("/") + "/fail"
    try:
        urlopen(target, timeout=10)
    except OSError as exc:
        emit(f"healthcheck ping failed: {exc}")


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

        hold_wake_assertion(os.getpid(), emit=emit)

        emit(f"=== scheduled run: job={args.job} partition={_YESTERDAY} ===")
        emit(f"repo={REPO}")
        emit(f"uv={UV}")
        emit(f"dagster_home={os.environ['DAGSTER_HOME']}")

        code = _run_chain(args.job, steps, log, emit)
        report_outcome(args.job, code, log_path, emit=emit)
        return code


def _run_chain(
    job: str,
    steps: list[list[str]],
    log: TextIO,
    emit: Callable[[str], None],
) -> int:
    """Run the preflight, pre-step, steps, and post-steps; return exit code."""
    emit("--- preflight: wait for network (DNS) ---")
    wait_for_network(emit=emit)

    # Materialize the locked environment up front, while the network (if
    # needed) is known-good, so the ``uv run`` steps below never touch
    # PyPI mid-chain. After a merge changes the tree, ``uv run`` rebuilds
    # the project and needs hatchling — this is where that now happens,
    # with retries instead of a silent skipped partition.
    # ``--inexact`` matters: a plain (exact) sync REMOVES packages outside
    # the default lockfile set, which strips the dev extras (ruff, mypy,
    # jupyter) from the shared venv on every scheduled run.
    sync_cmd = [UV, "sync", "--inexact"]
    emit(f"--- pre-step: {' '.join(sync_cmd)} ---")
    sync_code = run_step_with_retries(
        sync_cmd,
        label="pre-step uv sync",
        emit=emit,
        log_file=log,
        attempts=3,
        retry_delay_s=30.0,
    )
    if sync_code != 0:
        emit(f"PRE-STEP FAILED (exit {sync_code}): uv sync")
        return sync_code

    for index, step in enumerate(steps, start=1):
        # Invoke via ``python -m dagster`` (not the ``dagster`` console
        # script) so a stale entry-point shebang from a relocated/rebuilt
        # venv can't break the unattended run.
        cmd = [UV, "run", "python", "-m", "dagster", *step]
        emit(f"--- step {index}/{len(steps)}: {' '.join(cmd)} ---")
        code = run_step_with_retries(
            cmd,
            label=f"step {index}/{len(steps)}",
            emit=emit,
            log_file=log,
        )
        if code != 0:
            emit(f"FAILED (exit {code}) on step {index}")
            return code

    emit("=== all steps succeeded ===")

    post_steps = POST_STEPS.get(job, [])
    for index, cmd in enumerate(post_steps, start=1):
        emit(f"--- post-step {index}/{len(post_steps)}: {' '.join(cmd)} ---")
        code = run_step_with_retries(
            cmd,
            label=f"post-step {index}/{len(post_steps)}",
            emit=emit,
            log_file=log,
        )
        if code != 0:
            emit(f"POST-STEP FAILED (exit {code}) on step {index}")
            return code
    emit("=== all post-steps succeeded ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
