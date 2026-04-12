# Plan: Weather-Adjusted Generation Analytics Dashboard UI

> Source PRD: [issue #20](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/20)
> Source Spec: `docs/superpowers/specs/2026-04-11-dashboard-ui-design.md`
> CEO Review: HOLD SCOPE, 33 rigor amendments applied 2026-04-12

## Architectural decisions

Durable decisions that apply across all phases:

- **Stack**: Panel (HoloViz) + Bokeh, compiled to WASM via `panel convert --to pyodide-worker`. Runs entirely in the browser via Pyodide. No backend server.
- **Pyodide HTTP client**: `pyodide.http.pyfetch` (not `requests`). Data loader functions are `async def`; components await them via Panel's async support. Pyodide CDN is `cdn.jsdelivr.net` (default), supply-chain risk acknowledged and documented in module docstring.
- **Hosting**: Static files on charleslikesdata.com (GitHub Pages) under `/dashboard/`. WAGA repo owns development; portfolio repo owns hosting.
- **Panel app location**: `src/weather_analytics/dashboard/` with modular structure:
  - `app.py` — entry point, assembles `FastListTemplate` (sidebar disabled, filter bar in main area), applies Bokeh theme inside servable function (not at module top-level) to avoid test pollution
  - `data_loader.py` — async functions using `pyodide.http.pyfetch`, cached, return Polars frames
  - `components/filters.py`, `kpi_cards.py`, `fleet_view.py`, `asset_view.py`, `weather_view.py`
  - `components/_chart_helpers.py` — shared utilities (themed figure factory, empty-data guard, tooltip styler)
  - `theme.py` — Bokeh `Theme` object + CSS string (exported, not applied on import)
  - `static/portfolio.css` — Poppins import + portfolio palette
- **Dagster export asset split**: Two separate assets for clean separation of concerns:
  - `waga_dashboard_export_build` — queries marts, projects columns, writes local JSON files. Downstream of `mart_asset_performance_daily` and `mart_asset_weather_performance`. Raises `dagster.Failure` if either mart has fewer than 10 rows (matching `correlation.py:63-69` pattern).
  - `waga_dashboard_export_publish` — reads local JSON files, pushes to portfolio repo via GitHub Contents API. Downstream of `waga_dashboard_export_build`. Retry policy: `RetryPolicy(max_retries=2, delay=60)`.
- **Column case normalization**: Both assets must lowercase Snowflake column names before projection using the pattern from `correlation.py:60-61`: `raw_df = raw_df.rename({col: col.lower() for col in raw_df.columns})`. Skipping this will break at hour 1.
- **Schedule**: New daily schedule `waga_daily_dashboard` at 09:30 UTC (30 min after `waga_daily_dbt` completes). Targets both `_build` and `_publish` assets via `AssetSelection.assets(...)`.
- **Cross-repo push**: GitHub Contents API via `PyGithub`. No git clone/push (works from Dagster Cloud serverless). PyGithub debug logging MUST NOT be enabled (can leak PAT in headers).
- **Env vars / secrets**:
  - `WAGA_PORTFOLIO_REPO_OWNER` (e.g., `cdcoonce`)
  - `WAGA_PORTFOLIO_REPO_NAME` (e.g., `charleslikesdata`)
  - `WAGA_PORTFOLIO_REPO_BRANCH` (default `main`)
  - `WAGA_PORTFOLIO_REPO_TOKEN` (Dagster Cloud secret + GitHub Actions secret + local `.env`). Fine-grained PAT scoped to portfolio repo `contents:write`. Rotate quarterly. Note: fine-grained PATs cannot path-restrict within a repo — leaked PAT could touch any file in the portfolio repo. Accepted risk; mitigation is rotation + commit history review.
- **Data contract**: Four JSON files at `dashboard/data/` in portfolio repo:
  - `manifest.json` — metadata + schema version
  - `assets.json` — asset dimension table
  - `daily_performance.json` — projected from `mart_asset_performance_daily`
  - `weather_performance.json` — projected from `mart_asset_weather_performance`
- **Schema version**: `1.0`, stored in `manifest.json`. App reads at startup; mismatch shows non-blocking warning banner.
- **GitHub Action**: `.github/workflows/build-dashboard.yml`, path-filtered to `src/weather_analytics/dashboard/**`. Runs `panel convert`, smoke-tests that output files exist, then pushes static output to portfolio repo via PAT. Workflow declares `permissions: contents: read` for the WAGA repo (minimum blast radius).
- **Pyproject.toml deps**: `pygithub` in main `[project.dependencies]`. `panel` and `bokeh` in `[project.optional-dependencies] dashboard` extra. Dagster Cloud installs main deps only (faster cold starts). GitHub Action installs `uv sync --extra dashboard`.
- **mypy overrides**: Add `panel.*`, `bokeh.*`, `github.*` to `[[tool.mypy.overrides]] ignore_missing_imports = true` block.
- **filterwarnings**: Keep global `error::DeprecationWarning`. Add targeted per-package ignores (e.g., `ignore::DeprecationWarning:panel.*`) as they surface during implementation — do not disable the global error mode.
- **Tab structure**: Fleet Overview / Asset Deep-Dive / Weather Correlation, with shared filter bar and KPI row above.
- **Theme**: Four-layer (portfolio.css with CSS variables, Bokeh `Theme` for chart internals, desaturated data palette, widget CSS overrides). Poppins font weights 300/400/500/600 via Google Fonts.
- **Pyodide-compatible deps only**: `panel`, `bokeh`, `polars`, `numpy`. Documented constraint in dashboard module docstring.
- **Test conventions**: pytest markers `unit` and `integration` per existing WAGA convention. New tests live under `tests/unit/dashboard/` and `tests/unit/assets/analytics/`.
- **Local artifacts** (gitignored): `dashboard_exports/` (Dagster local copy of JSON), `dashboard_build/` (panel convert output).

## Pyodide kill criteria

If Phase 1 hits ANY of the following, stop Panel+Pyodide work and switch to Plotly static HTML exports as a replacement Phase 1:

1. Polars wheel fails to load in Pyodide (verified via `panel convert` + browser smoke test)
2. Initial WASM bundle + deps exceed 25MB (dashboard becomes unusable on slow connections)
3. `panel convert --to pyodide-worker` fails on the minimal Phase 1 app (can't produce a working build)
4. Bokeh charts fail to render in Pyodide with the theme applied (visual layer broken)

Fallback: Plotly static HTML exports. Dagster asset writes pre-rendered Plotly HTML files directly to the portfolio repo. No in-browser Python. Same data contract, simpler hosting. Skip Phase 3's reactive filter work (Plotly has client-side JS interactivity).

Do not grind on Pyodide issues beyond Phase 1. If the kill criteria trip, switch decisively and rewrite the remaining phases to use Plotly.

## Rollback strategy

If a bad data push or bad app build breaks the live dashboard:

1. `git revert` the offending commit on the portfolio repo (`cdcoonce/charleslikesdata`)
2. GitHub Pages auto-rebuilds within 1-2 minutes
3. Investigate root cause in WAGA repo; fix forward on next pipeline run or next push

During GitHub Pages rebuild after each data refresh (1-2 min), visitors see stale data. This is expected static-site behavior, not a bug.

---

## Phase 1: Tracer bullet — end-to-end with full automation

**User stories**: deployment pipeline, PAT auth, Panel + Pyodide proof, portfolio integration

### What to build

A minimal but production-quality vertical slice that proves the entire delivery mechanism end-to-end, before investing in real charts. The slice includes:

- Two new Dagster assets (`waga_dashboard_export_build` and `waga_dashboard_export_publish`) that together query one mart, project 2-3 columns with lowercase normalization, write a single JSON file locally, and push it to the portfolio repo's `dashboard/data/` directory via the GitHub Contents API.
- A minimal Panel app at `src/weather_analytics/dashboard/app.py` that fetches the JSON via `pyodide.http.pyfetch`, renders a single chart, and applies the theme inside the servable function.
- The four-layer theme infrastructure stub: `theme.py` exports `portfolio_theme`, `static/portfolio.css` imports Poppins and declares palette CSS variables.
- A `.github/workflows/build-dashboard.yml` GitHub Action with `permissions: contents: read`, path-filtered to `src/weather_analytics/dashboard/**`, running `uv sync --extra dashboard`, executing `panel convert`, smoke-testing that output files exist, and pushing to portfolio repo.
- A first-time setup checklist documented in the dev-cycle log, executed in the correct order (nav link added LAST).
- Minimal unit tests covering both assets' logic and the data loader's JSON parsing.

**This phase also serves as the Pyodide feasibility gate.** If the kill criteria trip during Phase 1, stop and switch to Plotly static HTML per the plan header.

### Acceptance criteria

- [ ] `panel`, `bokeh` added to `[project.optional-dependencies] dashboard` extra in `pyproject.toml`
- [ ] `pygithub` added to `[project.dependencies]`
- [ ] `panel.*`, `bokeh.*`, `github.*` added to `[[tool.mypy.overrides]] ignore_missing_imports` block
- [ ] Phase 1 records the verified Panel/Bokeh/Polars/Pyodide version combination in `pyproject.toml` and the dashboard module docstring; subsequent phases do not upgrade these without re-running `panel convert` smoke test
- [ ] `waga_dashboard_export_build` asset exists at `src/weather_analytics/assets/analytics/dashboard_export.py`, registered in `definitions.py`
- [ ] Build asset lowercases Snowflake column names before projection (per `correlation.py:60-61` pattern)
- [ ] Build asset raises `dagster.Failure` if mart has fewer than 10 rows (per `correlation.py:63-69`)
- [ ] Build asset logs per-step timing (`context.log.info("Snowflake query completed in Xms")`, etc.)
- [ ] Build asset writes one JSON file (e.g., `daily_performance.json` with 2-3 columns) to local `dashboard_exports/`
- [ ] `waga_dashboard_export_publish` asset exists, downstream of build asset, with `RetryPolicy(max_retries=2, delay=60)`
- [ ] Publish asset reads local files and pushes to `dashboard/data/` in portfolio repo via GitHub Contents API
- [ ] Publish asset raises `dagster.Failure` on any API error with the response logged (but NEVER the PAT)
- [ ] Publish asset logs per-step timing for each API call
- [ ] Publish asset emits asset metadata including `generated_at`, `pipeline_run_id`, commit SHA, byte sizes
- [ ] New schedule `waga_daily_dashboard` at 09:30 UTC targets both assets via `AssetSelection.assets(...)`, added to `schedules.py` and registered in `definitions.py`
- [ ] Phase 1 smoke tests confirm Dagster Cloud serverless can reach api.github.com from the asset
- [ ] Panel app at `src/weather_analytics/dashboard/app.py` has a `servable()` entry point that applies `portfolio_theme` inside the function (not at module import)
- [ ] Panel app uses `pyodide.http.pyfetch` (async) to load JSON; components await the loaders
- [ ] Panel app catches fetch errors and renders a "Data temporarily unavailable" banner with empty layout chrome
- [ ] Panel app emits `console.error()` on browser-side errors
- [ ] Panel app renders one chart with correct theme (Poppins font visible, charcoal axes, no Bokeh logo, hidden toolbar)
- [ ] `FastListTemplate` sidebar is disabled; filter bar and content live in the main area
- [ ] `static/portfolio.css` imports Poppins and declares all palette CSS variables from the spec
- [ ] `theme.py` exports a `portfolio_theme` Bokeh `Theme` object (does not apply to global config at import)
- [ ] `.github/workflows/build-dashboard.yml` exists with `permissions: contents: read`, triggers on push to main with path filter on `src/weather_analytics/dashboard/**`
- [ ] GitHub Action runs `uv sync --extra dashboard`, then `panel convert --to pyodide-worker`, then smoke-tests that `index.html`, `app.js`, `worker.js` exist, then pushes to portfolio repo
- [ ] PAT secret stored in Dagster Cloud, GitHub Actions, and local `.env`
- [ ] PyGithub debug logging NOT enabled anywhere
- [ ] Portfolio repo has `/dashboard/` directory with placeholder `index.html` (before nav link is added)
- [ ] First-time setup flow executed in correct order: PAT → secrets → deps → placeholder dir → implement code → manual build+push → enable automation → **nav link last**
- [ ] Portfolio nav has a "Dashboard" link pointing to `/dashboard/` (added AFTER working dashboard is deployed)
- [ ] Unit tests pass for both assets' logic, data loader JSON parsing, theme.py smoke test
- [ ] Unit tests pass for publish asset error conditions (401, 403, 404, rate limit, network timeout) using mocked PyGithub
- [ ] Manual smoke test: visit charleslikesdata.com/dashboard/ in Chrome, see the chart render with correct theme
- [ ] Pyodide kill criteria evaluated: if any trip, switch to Plotly static HTML plan before Phase 2

---

## Phase 2: Full data contract

**User stories**: data freshness, schema versioning, all mart columns surfaced

### What to build

Expand Phase 1's minimal export to the complete data contract specified in the design spec. The build asset projects the full 15-column subset from each mart, builds all four JSON files (manifest, assets, daily_performance, weather_performance). The publish asset pushes all four files to the portfolio repo in a single commit. The Panel app's data loader is expanded to fetch and cache all four files, exposing them as Polars `LazyFrame`s through async loaders. A schema version check at app startup compares the manifest's `schema_version` against `EXPECTED_SCHEMA_VERSION = "1.0"` and shows a non-blocking warning banner on mismatch. A new asset check `waga_dashboard_export_commit_landed` verifies the commit SHA resolves on the portfolio repo after the publish asset runs.

### Acceptance criteria

- [ ] Build asset writes all four JSON files: `manifest.json`, `assets.json`, `daily_performance.json`, `weather_performance.json`
- [ ] `daily_performance.json` includes the 15 columns specified in the design spec
- [ ] `weather_performance.json` includes the 11 columns specified in the design spec
- [ ] `assets.json` includes `asset_id`, `asset_type`, `capacity_mw`, `size_category`, `display_name` for all assets
- [ ] `manifest.json` includes `generated_at`, `pipeline_run_id`, `date_range`, `asset_count`, `row_counts`, `schema_version`
- [ ] Build asset raises `dagster.Failure` if EITHER mart has fewer than 10 rows (not just one)
- [ ] All four files are pushed in a single commit with message `chore(dashboard): refresh data YYYY-MM-DD [pipeline run abc123]`
- [ ] Total payload size is under 500KB raw / 100KB gzipped
- [ ] `data_loader.py` exposes async functions: `load_manifest()`, `load_assets()`, `load_daily_performance()`, `load_weather_performance()` returning Polars DataFrames/LazyFrames
- [ ] Data loader caches results in-memory so switching tabs does not refetch
- [ ] Data loader catches fetch errors per-file and surfaces them to the app-level error banner handler
- [ ] Panel app boots in two phases: data fetch completes → filters render and accept input. Loading spinner shown during initial fetch.
- [ ] Panel app calls `load_manifest()` at startup and compares `schema_version` to a constant `EXPECTED_SCHEMA_VERSION = "1.0"`
- [ ] On schema mismatch, app displays a warning banner at the top with text like "Data schema version X.Y does not match expected 1.0 — display may be incorrect" (non-blocking)
- [ ] `waga_dashboard_export_commit_landed` asset check exists at `src/weather_analytics/checks/dashboard.py` (or similar), runs after publish, fetches commit SHA via GitHub API, passes if resolved
- [ ] Asset check has a unit test with mocked PyGithub
- [ ] Integration test (mocked Snowflake + mocked PyGithub) verifies the full build+publish flow produces all four files with correct columns in a single commit
- [ ] Unit tests cover all data loader functions (mocked pyodide.http.pyfetch), schema version check logic, and manifest construction

---

## Phase 3: Filter bar + KPI row (shared chrome)

**User stories**: interactivity, reactive filtering, summary metrics

### What to build

The shared chrome that sits above all three tabs: a filter bar at the top with three controls (asset selector, date range, asset type toggle) and a KPI row below it with four cards. Filters are exposed as Panel `param` objects so downstream tab components can subscribe via `@pn.depends()`. KPI cards compute Total MWh, Avg Capacity Factor, Avg Availability, and Avg Performance Score from the filtered DataFrame. Card styling matches the portfolio's project-card style (10px border-radius, soft shadow, Poppins font, charcoal text).

### Acceptance criteria

- [ ] `components/filters.py` defines a `Filters` class with three reactive params: `asset_id` (single-select with "All"), `date_range` (start, end), `asset_type` (All/Wind/Solar)
- [ ] Asset selector populates from `assets.json` with `display_name` labels
- [ ] Date range picker defaults to the manifest's `date_range`
- [ ] Asset type toggle filters the asset selector (e.g., selecting Wind hides solar assets from the dropdown)
- [ ] When `asset_type` changes and currently selected `asset_id` doesn't match the new type, reset `asset_id` to "All"
- [ ] Filter widgets styled with Poppins font and pill-shaped (2rem border-radius) per the spec
- [ ] `components/kpi_cards.py` exposes `kpi_row(filters) -> pn.Row` that returns four reactive cards
- [ ] KPI cards compute: Total MWh (sum of `total_net_generation_mwh`), Avg Capacity Factor (mean of `daily_capacity_factor`), Avg Availability (mean of `avg_availability_pct`), Avg Performance Score (mean of `performance_score`)
- [ ] KPI cards re-render when any filter changes
- [ ] KPI cards handle empty filtered DataFrames by rendering "—" instead of crashing
- [ ] Card visual styling: white background, 10px border-radius, `0 4px 6px rgba(0,0,0,0.1)` shadow, Poppins, charcoal large value, gray uppercase label
- [ ] App layout shows filter bar → KPI row → empty tabs container
- [ ] Unit tests cover filter state mutations, asset-type conflict reset, KPI computation correctness, and empty-DataFrame edge cases
- [ ] Manual smoke test: filters update KPIs reactively in browser

---

## Phase 4: Fleet Overview tab

**User stories**: fleet-level operational view

### What to build

The first production tab and the shared chart helpers module. `components/_chart_helpers.py` is introduced with utilities extracted from the first real chart usage: `make_themed_figure(title, x_label, y_label)` for consistent Bokeh figure setup, `with_empty_guard(df, render_fn, message)` wrapping chart render functions to handle empty DataFrames, and `style_tooltip(fig, columns)` for consistent hover tooltip formatting.

`components/fleet_view.py` exposes `fleet_panel(filters) -> pn.Column` returning three charts that subscribe to the filter bar:

1. **Generation over time (stacked by asset)** — line or area chart showing daily `total_net_generation_mwh` per asset over the selected date range, legend per asset, wind/solar colors based on `inferred_asset_type`.
2. **Capacity factor by asset (bar chart)** — horizontal bar chart of average `daily_capacity_factor` per asset, sorted descending, bars colored by asset type.
3. **Performance score heatmap (asset × date)** — heatmap with asset_id rows, date columns, cells colored by `performance_score` using sequential palette.

### Acceptance criteria

- [ ] `components/_chart_helpers.py` exists with `make_themed_figure`, `with_empty_guard`, `style_tooltip` utilities
- [ ] All three charts use `make_themed_figure` for Bokeh figure instantiation
- [ ] All three charts wrap their render in `with_empty_guard` to handle empty filtered DataFrames
- [ ] `components/fleet_view.py` exists with `fleet_panel(filters) -> pn.Column`
- [ ] Generation-over-time chart renders with one series per asset
- [ ] Generation chart respects asset_id, date_range, and asset_type filters
- [ ] Capacity factor bar chart shows one bar per asset, sorted descending
- [ ] Performance heatmap uses date range as columns and asset_id as rows
- [ ] All three charts apply the global Bokeh theme (Poppins, charcoal, no toolbar logo)
- [ ] Wind assets render in `#4a7c7e`, solar in `#d4a44c`
- [ ] Hover tooltips show date, asset_id, and exact metric value via `style_tooltip`
- [ ] Tab added to `FastListTemplate` tabs container as the first tab labeled "Fleet Overview"
- [ ] Tab re-renders when any filter changes (verified manually)
- [ ] Unit tests cover data shaping logic per chart, empty-guard behavior, chart helper utilities
- [ ] Manual smoke test: switching filters updates all three Fleet Overview charts in browser

---

## Phase 5: Asset Deep-Dive tab

**User stories**: single-asset drill-down

### What to build

The second production tab. `components/asset_view.py` exposes `asset_panel(filters) -> pn.Column` returning four charts focused on a single selected asset. When "All" is selected, the tab shows a placeholder prompting the user to pick an asset:

1. **Daily generation: expected vs. actual** — line chart, `avg_actual_generation_mwh` (charcoal solid) and `avg_expected_generation_mwh` (gray dashed).
2. **Rolling capacity factor (7d / 30d)** — line chart, three series: daily `daily_capacity_factor` (light), `rolling_7d_avg_cf` (medium), `rolling_30d_avg_cf` (charcoal).
3. **Wind/solar scatter with regression line** — scatter plot of wind speed (or GHI) vs. generation, fitted regression line overlay using `wind_regression_slope/intercept` or `solar_regression_slope/intercept` based on `inferred_asset_type`, r² in title.
4. **Performance distribution hours (stacked bar)** — daily stacked bar showing `excellent_hours`, `good_hours`, `fair_hours`, `poor_hours` using the performance palette.

### Acceptance criteria

- [ ] `components/asset_view.py` exists with `asset_panel(filters) -> pn.Column`
- [ ] Tab shows a placeholder with instructions when `asset_id == "All"`
- [ ] All four charts use `make_themed_figure` and `with_empty_guard` from `_chart_helpers.py`
- [ ] Expected vs. actual chart renders both series with distinct styling (solid vs. dashed)
- [ ] Rolling CF chart renders three series with distinct line weights/colors
- [ ] Scatter chart auto-selects wind or solar variables based on `inferred_asset_type`
- [ ] Scatter chart includes a fitted regression line and r² in the title
- [ ] Performance distribution stacked bar uses the four performance category colors from the palette
- [ ] All charts respect the date_range filter
- [ ] Tab added to `FastListTemplate` tabs container as the second tab labeled "Asset Deep-Dive"
- [ ] Charts re-render when asset_id or date_range changes
- [ ] Unit tests cover data shaping for each chart and asset-type detection logic
- [ ] Manual smoke test: selecting different assets updates all four charts in browser

---

## Phase 6: Weather Correlation tab + final QA

**User stories**: weather-adjusted performance analysis

### What to build

The third and final production tab plus final visual QA. `components/weather_view.py` exposes `weather_panel(filters) -> pn.Column` returning three charts:

1. **Wind vs. solar r² matrix by asset** — grouped bar chart or heatmap showing each asset's `wind_r_squared` and `solar_r_squared` side by side.
2. **Wind speed vs. generation scatter** — scatter of `avg_wind_speed_mps` vs. `total_net_generation_mwh` from `daily_performance.json`, colored by asset, filtered to wind assets only.
3. **GHI vs. generation scatter** — scatter of `avg_ghi` vs. `total_net_generation_mwh` from `daily_performance.json`, colored by asset, filtered to solar assets only.

Final visual QA pass: Chrome + Safari, theme match vs. portfolio, load time verification, warning banner verification.

### Acceptance criteria

- [ ] `components/weather_view.py` exists with `weather_panel(filters) -> pn.Column`
- [ ] All three charts use `make_themed_figure` and `with_empty_guard`
- [ ] Wind/solar r² matrix renders with grouped bars or heatmap
- [ ] Wind speed scatter shows only wind assets, colored per wind palette
- [ ] GHI scatter shows only solar assets, colored per solar palette
- [ ] All three charts respect the date_range filter
- [ ] Tab added to `FastListTemplate` tabs container as the third tab labeled "Weather Correlation"
- [ ] Tab re-renders when filters change
- [ ] Unit tests cover data shaping for each chart
- [ ] Visual QA: theme matches portfolio site palette, font, shadows, and card styling in Chrome
- [ ] Visual QA: theme renders acceptably in Safari (no broken layout)
- [ ] Performance check: returning visitor (Pyodide cached) loads dashboard in under 5 seconds
- [ ] Performance check: first-time visitor loads in under 15 seconds on a fast connection
- [ ] Schema version mismatch warning banner verified by manually editing `manifest.json` to a fake version
- [ ] Data-unavailable banner verified by temporarily pointing data loader at a 404 URL
- [ ] All acceptance criteria from PRD issue #20 are checked off
- [ ] Final manual smoke test of all three tabs end-to-end in browser

---

## Deferred enhancements (out of scope for v1)

Listed in the design spec and here for traceability. These are explicit non-goals for the initial release:

- Data quality panel (validity %, anomaly counts)
- Dark mode toggle
- PNG export of charts
- Deep-link URL params for sharing specific views
- Metrics catalog page listing every metric with its definition and source
- Dagster sensor for consecutive failures alerting
- Playwright E2E smoke test in GitHub Action
