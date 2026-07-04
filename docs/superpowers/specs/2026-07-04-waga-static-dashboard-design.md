# WAGA Static Dashboard — Design (in progress)

**Date:** 2026-07-04
**Status:** DESIGN — awaiting resolution of 2 open questions (see end), then spec finalize → writing-plans.
**Context:** written just before a `/compact`, to preserve the brainstorming state.

## Goal

Replace WAGA's heavy client-side **Panel + Pyodide** dashboard (`src/weather_analytics/dashboard/`)
with a **static, server-rendered dashboard** in the style of `afk-cockpit`, deployed to its **own
Cloudflare Pages project** (`*.pages.dev`). Public (privacy is not blocking — confirmed).

## Decisions locked (from brainstorming Q&A)

1. **Hosting:** its own Cloudflare Pages project via `wrangler pages deploy` (mirrors
   `afk-cockpit/src/afk_cockpit/cloudflare.py`). Not a portfolio subpath. The portfolio WAGA
   project card's `href` is repointed to the new `*.pages.dev` URL.
2. **Interactivity:** *static + light interactivity*. Server-rendered default view, plus a small
   vanilla-JS layer over a baked-in JSON data island — an **asset/type filter** (wind/solar/individual)
   and a **date-range** toggle that recompute KPIs and redraw the SVG charts client-side. No runtime,
   single self-contained file.
3. **Pattern:** afk-cockpit's `charts → render` approach — inline SVG/CSS geometry generated in
   Python (NO chart library, NO Plotly, NO Pyodide), Jinja2 template → one self-contained
   `dist/index.html` (~afk-cockpit is 46 KB).

## Reference: afk-cockpit (the template to mirror)

Repo: `/Users/cdcoonce/Developer/GitHub/afk-cockpit`. Modules:
`ingest → enrich → marts → charts → render → publish/cloudflare`, plus `cli.py`, `serve.py`,
`config.py`. `charts.py` returns SVG path strings / polyline points / bar geometry; `render.py`
fills `templates/index.html.j2` into a single static `dist/index.html`; `cloudflare.py` runs
`wrangler pages deploy dist --branch main`. Full pytest suite (`test_charts`, `test_render`,
`test_publish`, `test_cloudflare`, `test_serve`, `test_cli`, ...).

## WAGA data contract (the 4 export JSONs, already produced by `waga_dashboard_export_build`)

Source: `dashboard_exports/*.json` (from Snowflake MARTS via the existing Dagster export asset).

- `manifest.json` — `{generated_at, pipeline_run_id, date_range:{start,end}, asset_count, row_counts, schema_version}`
- `assets.json` — list of 10 `{asset_id, capacity_mw, size_category, asset_type: wind|solar, display_name}`
- `daily_performance.json` — ~710 rows `{asset_id, date, total_net_generation_mwh, daily_capacity_factor, avg_availability_pct, total_curtailment_mwh, ...}`
- `weather_performance.json` — ~640 rows `{asset_id, date, performance_score, performance_category, avg_expected_generation_mwh, avg_actual_generation_mwh, ...}`

## Proposed architecture

New module `src/weather_analytics/cockpit/` replacing `src/weather_analytics/dashboard/`:

- `data.py` — load + normalize the 4 export JSONs into typed structures (Polars or plain dicts).
- `charts.py` — pure functions → inline-SVG geometry + KPI aggregates: fleet capacity factor,
  total net generation, avg weather-adjusted performance score, total curtailment; time-series
  polyline/area for generation & capacity factor; per-asset bars; wind-vs-solar split.
- `render.py` + `templates/index.html.j2` — Jinja → self-contained `dist/index.html`; embeds the
  full dataset as a `<script type="application/json">` island and server-renders the default view.
- `static/app.js` (inlined at build) — vanilla JS: on filter/date-range change, recompute KPIs and
  redraw the SVG charts from the baked JSON.
- `deploy.py` — `wrangler pages deploy dist --project-name <waga>` (mirrors afk-cockpit cloudflare.py).
- CLI: `python -m weather_analytics.cockpit build|deploy|serve`.

**Data flow:** Snowflake marts → `waga_dashboard_export_build` (existing) writes the 4 JSONs →
cockpit `render` → `dist/index.html` → `wrangler` → `*.pages.dev`.

**Views (mirror the current Panel dashboard, static):** fleet KPI header; generation-trend chart;
capacity-factor + weather-adjusted-performance charts; per-asset table; filter controls
(asset type, individual asset, date range).

## Removals / migration

- Delete `src/weather_analytics/dashboard/` (Panel/Pyodide) and its components.
- Drop the `dashboard` optional-deps extra (panel/bokeh/pyodide) from `pyproject.toml`.
- Retire `scripts/build_dashboard_app.py`, `scripts/push_dashboard_build.py`, and
  `.github/workflows/build-dashboard.yml` (the stale portfolio-`master` push path — the bug that
  motivated re-pointing hosting to Cloudflare in the first place).
- Repoint the portfolio `Weather-Adjusted Generation Analytics` project card `href` → new pages.dev URL
  (in `Portfolio_Website` `src/data/portfolio.js`; currently cockpit-slug `waga`).

## Testing (mirror afk-cockpit)

pytest: `charts` (geometry + KPI math), `render` (expected sections present, valid HTML, data island
embedded), `data` (JSON parse/normalize), `deploy` (wrangler argv construction, mocked — no real deploy).

## Orchestration — RECOMMENDATION (confirm)

Make **render** a Dagster asset (`waga_dashboard_render`, replacing the old `waga_dashboard_export_publish`)
wired into the **daily launchd chain** (re-enabling the dashboard step deferred in Thread 1, now
targeting Cloudflare instead of the stale portfolio `master`). Keep **deploy** as a separate
`wrangler` step/CLI so the pipeline doesn't carry deploy credentials. Auto-refreshes as the data
refreshes.

## OPEN QUESTIONS (resolve after /compact, before finalizing spec)

1. **Orchestration** — approve the recommendation above (render = Dagster asset in the daily chain;
   deploy = separate wrangler step), or prefer a fully standalone afk-cockpit-style CLI outside Dagster?
2. **Cloudflare setup** — is there already a CF Pages project + `CLOUDFLARE_API_TOKEN` for WAGA, or
   should the implementation plan include creating the Pages project and wiring the token into `.env`?

## Prior-thread context (for a post-compact session)

- **Thread 1 (launchd scheduling) — DONE, committed, NOT merged, NOT loaded.** Branch
  `feat/local-launchd-scheduling` in **both** WAGA (`1dbe790`) and `oura-pipeline` (`289cdbe`):
  `scripts/run_scheduled.py` + `scripts/install_launchd.py` + `docs/local-scheduling.md`, Dagster
  Cloud CI neutered to `workflow_dispatch`-only. WAGA daily was **foreground-tested green** (real
  Snowflake write, data refreshed to 2026-07-03). **Oura NOT foreground-tested** — may hide an
  `.env`-drift gap (WAGA's run surfaced missing `WAGA_DLT_*`; Oura only had `DAGSTER_HOME` +
  `SNOWFLAKE_DATABASE` fixed so far). launchd agents are **not loaded** — `install_launchd.py
  install --load` is the user's gated step. Oura has **pre-existing March uncommitted work**
  (`CLAUDE.md`, `docs/archive/*`) deliberately left unstaged.
- **Portfolio (charleslikesdata.com):** already updated + shipped to production this session
  (gallery restore, Work filtering, Contact rework). The WAGA card currently renders a cockpit SVG
  (slug `waga`) and links to the GitHub repo; this design repoints it to the pages.dev dashboard.
