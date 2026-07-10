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

Before the Dagster steps, the runner hardens itself against the
network-at-wake failure mode ("fixed failure mode 1" in the troubleshooting
section — the npx PATH failure, "fixed failure mode 2", is fixed in the
*plist* by the installer, not in the runner, and needs the agents reinstalled):

1. **Wake assertion** — spawns a `caffeinate -i -s -w <pid>` sidecar so the
   machine stays awake for the whole run. Without it, a DarkWake-fired job is
   suspended when the machine re-sleeps ~3 minutes later and resumes in
   ~3-minute slices every ~16 minutes — a ~10-minute chain was taking
   **3.5 hours** of wall-clock (observed 2026-07-09). The sidecar exits with
   the runner, releasing the assertion.
2. **Network preflight** — waits up to 5 minutes for DNS to resolve
   (`pypi.org`), probing every 15 s. launchd fires a missed job immediately on
   wake, which can be before the network is up. A timeout does **not** abort
   the run (an unchanged tree needs no network); it just logs and proceeds.
3. **`uv sync --inexact` pre-step** — materializes the locked environment up
   front (3 attempts, 30 s apart), so the `uv run` steps never touch PyPI
   mid-chain. A final failure aborts the chain (`PRE-STEP FAILED`).
   `--inexact` because an exact sync would *remove* the dev extras (ruff,
   mypy, jupyter) from the shared venv on every scheduled run.

Steps then run in order, each with **one retry after 60 s** on non-zero exit
(safe: dlt merges are idempotent, dbt builds are re-runnable, the cockpit
build/deploy just re-renders and re-uploads). A step that fails both attempts
**aborts the rest of the chain** and is logged as `FAILED (exit N)`.

After the Dagster steps, the `daily` job runs two **post-steps** (the
`POST_STEPS` dict, same retry policy): `cockpit build` renders the static
dashboard from the fresh JSON exports, and `cockpit deploy` publishes it to
Cloudflare Pages via `npx wrangler`. Unattended deploys are intentional — the
portfolio dashboard tracks the warehouse daily. (The old
`waga_dashboard_export_publish` asset, which pushed to a stale portfolio
branch, was removed when the cockpit replaced the Pyodide dashboard.)

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

## Alerting

The runner reports every run's outcome beyond the log file:

- **On failure**, it posts a macOS notification (`osascript`) — visible the
  next time you look at the machine.
- **Dead-man's switch (optional):** set `WAGA_HEALTHCHECK_URL` in `.env` to a
  [healthchecks.io](https://healthchecks.io)-style check URL. Every run pings
  it — `GET <url>` on success, `GET <url>/fail` on failure. Because a machine
  that never ran pings *nothing*, a missed ping is itself the alert: this
  catches launchd never firing (asleep all day, unloaded agent) — the one
  failure class no local signal can. Create a check with a ~26 h grace period
  and paste its URL into `.env`; until then the ping is a silent no-op.

## Health check & troubleshooting

Local signals to check by hand:

```sh
# Second column is the LAST EXIT CODE per agent: 0 = healthy, 1 = last run failed
launchctl list | grep waga

# The only reliable green signal is the last line of the newest log:
tail -1 "$(ls -t logs/scheduled-daily-*.log | head -1)"
# healthy => "=== all post-steps succeeded ==="
```

> **Do not judge health by log size.** Before 2026-07-10 every ~43 KB daily
> log *looked* healthy but had failed at post-step 2 (`cockpit deploy`,
> `FileNotFoundError: 'npx'`) — the Dagster steps' output dominates the file
> either way. Check the tail, not the size.

### Fixed failure mode 1: no network at wake (observed 2026-07-10)

`launchd` fires the job at 06:00 local — or immediately on wake if the machine
was asleep — which can be **before the network/DNS is up**. If the source tree
changed since the last run (e.g. PRs merged the day before), `uv run` rebuilds
the project and needs PyPI (`hatchling`); with no DNS the build fails:

```
├─▶ Failed to fetch: `https://pypi.org/simple/hatchling/`
╰─▶ failed to lookup address information: nodename nor servname provided
```

Before hardening, step 1 aborted the whole chain and the partition was
silently skipped (exit code 1 in `launchctl list`, tiny log file).
**Fixed 2026-07-10** by the network preflight + retryable `uv sync` pre-step
described above. Remaining (unimplemented) fallback if it ever recurs anyway:
a later `StartCalendarInterval` (e.g. 07:00) to widen the wake-to-network gap.

### Fixed failure mode 2: `npx` not on the agent PATH (every run before 2026-07-10)

The `cockpit deploy` post-step shells out to `npx wrangler`, but the agents'
minimal `PATH` did not include the node bin dir (`/opt/homebrew/bin`), so
**every unattended deploy failed** with `FileNotFoundError: 'npx'` while the
Dagster steps succeeded — partitions landed, the published dashboard went
stale, and the exit code was 1 either way. **Fixed 2026-07-10:**
`install_launchd.py` now resolves `npx` at install time and adds its directory
to the agent PATH (with an install-time warning if `npx` doesn't resolve —
the `/opt/homebrew/bin` fallback is only right for Homebrew-managed node).
This lives in the **plist**, so after pulling the fix you must rewrite +
reload the agents:

```sh
# reloading UNLOADS each agent first, which kills an in-flight run mid-chain —
# make sure nothing is running (daily runs can take ~2 h from 06:00):
pgrep -fl run_scheduled.py || uv run python scripts/install_launchd.py install --load
```

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
