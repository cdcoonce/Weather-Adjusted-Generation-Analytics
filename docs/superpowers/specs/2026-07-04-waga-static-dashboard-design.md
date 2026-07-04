# WAGA Static Dashboard — Design

**Date:** 2026-07-04
**Status:** DESIGN — finalized. Both open questions resolved (standalone CLI; plan
creates the Cloudflare Pages project). Ready for user review → writing-plans.

## Goal

Replace WAGA's client-side **Panel + Bokeh + Pyodide** dashboard
(`src/weather_analytics/dashboard/`) with a **static, server-rendered dashboard** in the
style of `afk-cockpit`, deployed to its **own Cloudflare Pages project** (`*.pages.dev`).
Public (privacy is not blocking — confirmed). This also retires the stale
push-to-portfolio-`master` publish path that motivated re-hosting in the first place.

## Decisions locked

1. **Hosting:** its own Cloudflare Pages project via `npx --yes wrangler pages deploy`,
   mirroring `afk-cockpit/src/afk_cockpit/cloudflare.py` exactly. Not a portfolio subpath.
   The portfolio WAGA project-card `href` is repointed to the new `*.pages.dev` URL.
2. **Interactivity:** *static + light interactivity*. Server-rendered default view, plus a
   small vanilla-JS layer over a baked-in JSON data island — an **asset-type/individual-asset
   filter** and a **date-range** toggle that recompute KPIs and redraw the SVG charts
   client-side. No runtime, single self-contained file.
3. **Pattern:** afk-cockpit's `data → charts → render` approach — inline SVG/CSS geometry
   generated in Python (NO chart library, NO Bokeh, NO Pyodide), Jinja2 template → one
   self-contained `dist/index.html`.
4. **Orchestration (RESOLVED):** cockpit is a **standalone module + CLI**
   (`build`/`deploy`/`serve`), NOT a Dagster asset — mirroring afk-cockpit and household-bms,
   which both keep the dashboard standalone precisely so render/serve need no Dagster context.
   The daily launchd chain runs it as plain steps *after* the Dagster export asset.
5. **Cloudflare setup (RESOLVED):** WAGA has **no** existing CF Pages project, `wrangler.toml`,
   or `CLOUDFLARE_API_TOKEN` (verified — the only cloudflare reference in the repo is this
   design doc). The implementation therefore **creates** the Pages project and wires
   `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID` into `.env` (both read automatically by
   `wrangler`). Creating the CF token/project in the Cloudflare dashboard is a **gated step for
   Charles**; the plan documents it and everything else is code.

## Prerequisite / sequencing (IMPORTANT)

The deploy step is appended to `scripts/run_scheduled.py`, which exists **only** on
`feat/local-launchd-scheduling` (Thread 1 — done, foreground-tested green on WAGA daily, but
**unmerged**; `main` and this design's `feat/static-dashboard` branch do not have it). So the
Thread 2 implementation branch must be **based on `feat/local-launchd-scheduling`** (or that
branch is merged to `main` first, then Thread 2 branches from `main`). Merging Thread 1 is a
gated step for Charles. Recommended order: merge Thread 1 → branch Thread 2 from `main`.

## Reference: afk-cockpit (the template to mirror)

Repo: `/Users/cdcoonce/Developer/GitHub/afk-cockpit`. `charts.py` returns SVG path strings /
polyline points / bar geometry; `render.py` fills `templates/index.html.j2` into a single
static `dist/index.html`; `cloudflare.py` runs
`npx --yes wrangler pages deploy <dir> --project-name <name> --branch main --commit-dirty=true`
(uses `npx`, not bare `wrangler`, because launchd's minimal PATH won't resolve a global
install). Full pytest suite (`test_charts`, `test_render`, `test_cloudflare`, `test_serve`,
`test_cli`, ...).

## WAGA data contract (the 4 export JSONs)

Produced by the existing Dagster asset **`waga_dashboard_export_build`** in
`src/weather_analytics/assets/analytics/dashboard_export.py` (queries
`WAGA.MARTS.mart_asset_performance_daily` and `WAGA.MARTS.mart_asset_weather_performance`,
lowercases columns, writes to `dashboard_exports/`). This asset is **kept** — it is the whole
interface between the pipeline and the static renderer. NaN → `null`; dates → ISO strings.

- **`manifest.json`** — `generated_at`, `pipeline_run_id`, `date_range:{start,end}`,
  `asset_count`, `row_counts:{daily_performance,weather_performance}`, `schema_version` ("1.0")
- **`assets.json`** — array of `{asset_id, capacity_mw, size_category, asset_type (wind|solar),
  display_name}`
- **`daily_performance.json`** — array (asset-date), 15 fields: `asset_id`, `date`,
  `total_net_generation_mwh`, `daily_capacity_factor`, `avg_availability_pct`,
  `total_curtailment_mwh`, `daily_performance_rating`, `excellent_hours`, `good_hours`,
  `fair_hours`, `poor_hours`, `avg_wind_speed_mps`, `avg_ghi`, `avg_temperature_c`,
  `data_completeness_pct`
- **`weather_performance.json`** — array (asset-date), 12 fields: `asset_id`, `date`,
  `performance_score`, `performance_category`, `avg_expected_generation_mwh`,
  `avg_actual_generation_mwh`, `avg_performance_ratio_pct`, `wind_r_squared`,
  `solar_r_squared`, `inferred_asset_type`, `rolling_7d_avg_cf`, `rolling_30d_avg_cf`

## Proposed architecture

New standalone module `src/weather_analytics/cockpit/` (mirrors afk-cockpit's module layout):

- `data.py` — load + normalize the 4 export JSONs into typed structures (plain dataclasses /
  dicts; no Snowflake, no Dagster).
- `charts.py` — pure functions → inline-SVG geometry + KPI aggregates: fleet capacity factor,
  total net generation, avg weather-adjusted `performance_score`, total curtailment;
  time-series polyline/area for generation & capacity factor; per-asset bars; wind-vs-solar
  split.
- `render.py` + `templates/index.html.j2` — Jinja → self-contained `dist/index.html`; embeds
  the full dataset as a `<script type="application/json">` island and server-renders the
  default view.
- `static/app.js` (inlined at build) — vanilla JS: on filter/date-range change, recompute KPIs
  and redraw the SVG charts from the baked JSON.
- `cloudflare.py` — `deploy(dist_dir, project_name)` running `npx --yes wrangler pages deploy`
  (copied from afk-cockpit, `DEFAULT_PROJECT_NAME = "waga-dashboard"`).
- `serve.py` — local static server for `dist/` (dev preview, mirrors afk-cockpit).
- `config.py` — project name / paths.
- `cli.py` + `__main__.py` — `python -m weather_analytics.cockpit build|deploy|serve`.

**Data flow:** Snowflake marts → `waga_dashboard_export_build` (Dagster, existing) writes the
4 JSONs → `cockpit build` renders `dist/index.html` → `cockpit deploy` (`wrangler`) →
`*.pages.dev`.

**Views (mirror the current Panel dashboard, static):** fleet KPI header; generation-trend
chart; capacity-factor + weather-adjusted-performance charts; per-asset table; filter controls
(asset type, individual asset, date range).

## Orchestration — the daily launchd chain

Re-enable the dashboard step deferred in Thread 1, now targeting Cloudflare. Append to the
`daily` job in `scripts/run_scheduled.py` (currently `[ingestion, dbt]`) so it becomes, in
order:

1. `dagster asset materialize --select waga_weather_ingestion,waga_generation_ingestion --partition <yesterday>` *(existing)*
2. `dagster asset materialize --select group:default` *(existing — dbt marts)*
3. `dagster asset materialize --select waga_dashboard_export_build` *(re-enabled: build only, writes the 4 JSONs)*
4. `python -m weather_analytics.cockpit build` *(new: JSONs → dist/index.html)*
5. `python -m weather_analytics.cockpit deploy` *(new: wrangler → pages.dev)*

Steps 1–3 stay `uv run python -m dagster ...` (the module form Thread 1 standardized on).
Deploy stays a plain CLI step so the pipeline carries no deploy credentials beyond the two CF
env vars `wrangler` reads. Auto-refreshes as the data refreshes.

## Removals / migration

- **Delete** `src/weather_analytics/dashboard/` entirely (Panel/Bokeh/Pyodide): `__init__.py`,
  `app.py`, `theme.py`, `data_loader.py`, `static/`, and
  `components/{_chart_helpers,asset_view,filters,fleet_view,kpi_cards,weather_view}.py`.
- **Remove** the `waga_dashboard_export_publish` asset (in `dashboard_export.py`) — it pushes
  the JSONs to the portfolio repo via the GitHub Trees API, obsolete once we deploy to
  Cloudflare. **Keep** `waga_dashboard_export_build`. Also drop the now-unused
  `PortfolioRepoResource` (used only by the publish asset) and its wiring in the Dagster
  `Definitions`, so nothing references the dead resource.
- **Remove** the old Dagster-Cloud `waga_daily_dashboard_schedule` from
  `src/weather_analytics/schedules.py` (it selects the now-deleted publish asset, so it would
  break `dagster definitions validate` otherwise). Thread 1 already retired the Dagster-Cloud
  scheduler in favor of launchd; this removes the last dangling reference.
- **Retire** `scripts/build_dashboard_app.py`, `scripts/push_dashboard_build.py`, and
  `.github/workflows/build-dashboard.yml` (the stale push-to-portfolio-`master` path).
- **Drop** the `dashboard` optional-deps extra from `pyproject.toml` (currently
  `dashboard = ["panel>=1.6.0,<1.7.0", "bokeh>=3.5.0,<3.8.0"]`).
- **Dead env keys:** `WAGA_PORTFOLIO_REPO_{OWNER,NAME,BRANCH,TOKEN}` become unused once
  `waga_dashboard_export_publish` is gone — remove from `.env.example` (and note in `.env`).
- **Add env keys:** `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID` to `.env.example` (+ real
  values in gitignored `.env`).
- **Repoint** the portfolio `Weather-Adjusted Generation Analytics` project-card `href` → new
  `*.pages.dev` URL (in `Portfolio_Website` `src/data/portfolio.js`; currently cockpit-slug
  `waga`). The portfolio card keeps its cockpit-SVG hero; only the link target changes.

## Testing (mirror afk-cockpit)

pytest under `tests/`:

- `test_data` — JSON parse/normalize; missing-field & empty-dataset handling.
- `test_charts` — geometry (SVG path/point strings) + KPI math on fixture data.
- `test_render` — expected sections present, valid/parseable HTML, data island embedded,
  single self-contained file (no external asset refs).
- `test_cloudflare` — `wrangler` argv construction with a mocked runner (no real deploy).
- `test_cli` / `test_serve` — subcommand dispatch; local server returns `dist/index.html`.

Fixture: a trimmed copy of the 4 JSONs (a few assets × a few dates) committed under
`tests/fixtures/`.

## Open questions

None — both resolved (see Decisions locked #4 and #5).
