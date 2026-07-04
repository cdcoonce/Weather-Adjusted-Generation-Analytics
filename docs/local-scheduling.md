# Local Scheduling (launchd)

## Why

Dagster Cloud (`dagster.plus`) was **retired** for this project, and with it the
hosted schedules that used to materialize the pipeline every day. The schedules
defined in `src/weather_analytics/schedules.py` still exist for use with the
local Dagster UI, but nothing runs them unattended anymore.

This directory's harness replaces those hosted schedules with macOS
**`launchd`** agents that invoke the pipeline locally on a calendar interval —
no external control plane, no API token, just a couple of user-level launch
agents on the machine that owns the `.env`.

## What launchd runs

Each launchd agent runs `scripts/run_scheduled.py <job>`, which chains one or
more `uv run python -m dagster asset materialize` steps against **yesterday's**
partition — computed in Python (UTC, to match the assets' partition timezone),
not the BSD `date` binary, and invoked via the `python -m` module form so a
stale venv entry-point can't break unattended runs. The job definitions live in
the `JOBS` dict in that script.

| Job      | Steps (in order)                                                                 | Assets materialized                                            |
| -------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `daily`  | 1. `--select waga_weather_ingestion,waga_generation_ingestion --partition <yday>`<br>2. `--select group:default` | The two ingestion assets (partitioned), then the dbt models (`group:default`: `stg_*`, `mart_*`, `metricflow_time_spine`) |
| `weekly` | 1. `--select waga_correlation_analysis`                                           | The correlation analysis asset                                |

Steps run in order; a non-zero exit **aborts the rest of the chain** and is
logged as `FAILED (exit N)`.

> **Deliberately excluded:** the dashboard-export assets
> (`waga_dashboard_export_build` / `waga_dashboard_export_publish`) are **not**
> scheduled here. They publish to a stale portfolio `master` target and are
> deferred to a separate thread. Do not add them to `JOBS` without re-pointing
> that target first.

## Schedule (local time)

`launchd` interprets these `StartCalendarInterval` values in the machine's
**local** timezone. They live in the `SCHEDULES` dict in
`scripts/install_launchd.py`.

| Job      | When (local)                | launchd label                          |
| -------- | --------------------------- | -------------------------------------- |
| `daily`  | every day at **06:00**      | `com.charleslikesdata.waga.daily`      |
| `weekly` | **Mondays** at **06:30**    | `com.charleslikesdata.waga.weekly`     |

## Install

The installer is macOS-only and, by default, only **writes** the plists — it
never loads an agent unless you explicitly ask.

```sh
# 1. Preview the exact plists that would be written (writes nothing):
uv run python scripts/install_launchd.py dry-run

# 2. Write the plists to ~/Library/LaunchAgents (does NOT activate them):
uv run python scripts/install_launchd.py install

# 3. Activate them now (writes AND launchctl-loads each agent):
uv run python scripts/install_launchd.py install --load
```

If you ran plain `install` (no `--load`) and want to activate later, load each
plist manually:

```sh
launchctl load ~/Library/LaunchAgents/com.charleslikesdata.waga.daily.plist
launchctl load ~/Library/LaunchAgents/com.charleslikesdata.waga.weekly.plist
```

## Uninstall

```sh
uv run python scripts/install_launchd.py uninstall
```

This unloads each agent (ignoring "not loaded" errors) and removes its plist
from `~/Library/LaunchAgents`.

## Logs

- **Per-run logs** from `run_scheduled.py`:
  `logs/scheduled-<job>-<YYYYMMDD-HHMMSS>.log` — one file per invocation, with
  the full step chain and Dagster CLI output.
- **launchd stdout/stderr** (agent-level):
  `logs/com.charleslikesdata.waga.<job>.out.log` and `...err.log`.

The `logs/` directory is gitignored, so nothing here is committed.

## dbt manifest (first-run note)

The dbt assets in `group:default` only appear **after** a dbt manifest has been
generated — until then `waga_dbt_assets` is `None` and the `group:default`
selection resolves to zero assets (see the manifest guard in
`src/weather_analytics/assets/dbt_assets.py`). Generate the manifest once before
relying on the `daily` job:

```sh
# Either run the full local UI once (it parses dbt on load)…
uv run dagster dev

# …or run one job manually, which will parse dbt as part of the chain:
uv run python scripts/run_scheduled.py daily
```

After that, `uv run dagster asset list --select "group:default" -m
weather_analytics.definitions` lists the staging + mart models.

## Dagster UI is still available

Retiring Dagster Cloud did **not** remove the local UI. Launch it any time to
inspect assets, kick off manual runs, or view run history:

```sh
uv run dagster dev
```

The schedules in `schedules.py` show up there as usual; they are just no longer
executed by a hosted scheduler — `launchd` owns unattended execution now.
