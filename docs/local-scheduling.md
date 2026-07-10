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
| `daily`  | 1. `--select waga_weather_ingestion,waga_generation_ingestion --partition <yday>`<br>2. `--select group:default`<br>3. `--select waga_dashboard_export_build` | The two ingestion assets (partitioned), then the dbt models (`group:default`: `stg_*`, `mart_*`, `metricflow_time_spine`), then the cockpit dashboard-export build |
| `weekly` | 1. `--select waga_correlation_analysis`                                           | The correlation analysis asset                                |

Steps run in order; a non-zero exit **aborts the rest of the chain** and is
logged as `FAILED (exit N)`.

> **Deliberately excluded:** `waga_dashboard_export_publish` is **not**
> scheduled here — publishing to Cloudflare Pages stays a manual
> `cockpit deploy` decision. (The export **build** was wired into the daily
> chain when the cockpit replaced the Pyodide dashboard.)

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

## Health check & troubleshooting

Nothing alerts on a failed run — the only signals are local. Check health with:

```sh
# Second column is the LAST EXIT CODE per agent: 0 = healthy, 1 = last run failed
launchctl list | grep waga

# A healthy daily log is ~40 KB; a failed one is typically ~1 KB
ls -la logs/scheduled-daily-*.log | tail -5
```

### Known failure mode: no network at wake (observed 2026-07-10)

`launchd` fires the job at 06:00 local — or immediately on wake if the machine
was asleep — which can be **before the network/DNS is up**. If the source tree
changed since the last run (e.g. PRs merged the day before), `uv run` rebuilds
the project and needs PyPI (`hatchling`); with no DNS the build fails and
step 1 aborts the whole chain:

```
├─▶ Failed to fetch: `https://pypi.org/simple/hatchling/`
╰─▶ failed to lookup address information: nodename nor servname provided
```

The partition for that day is then silently skipped (exit code 1 in
`launchctl list`, tiny log file). If the tree did NOT change, `uv run` needs no
network and the run is immune.

### Catch-up runbook (missed partition)

For a miss caught the **same day**, just re-run the job — it targets
yesterday's partition:

```sh
uv run python scripts/run_scheduled.py daily
```

For **older** gaps, materialize each missed date explicitly (always BOTH
ingestion assets per partition — a partition refreshed on only one asset mixes
weather/generation seeds in the mart joins), then the dbt models once:

```sh
uv run dagster asset materialize -m weather_analytics.definitions \
  --select waga_weather_ingestion,waga_generation_ingestion --partition <YYYY-MM-DD>
# ...repeat per missed date, then:
uv run dagster asset materialize -m weather_analytics.definitions --select group:default
```

Find gaps by comparing `logs/scheduled-daily-*.log` dates, or directly in the
warehouse: `select distinct timestamp::date from WAGA.raw.generation order by 1`.

### Hardening candidates (not implemented)

- Retry with backoff inside `run_scheduled.py` (a 5-minute retry would have
  covered the observed DNS-at-wake failure).
- A later `StartCalendarInterval` (e.g. 07:00) to widen the wake-to-network gap.
- `uv sync` as a separate, retryable pre-step so the materialize steps
  themselves never need the network.

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
