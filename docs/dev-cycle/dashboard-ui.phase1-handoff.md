# Dashboard UI — Phase 1 Handoff Notes

**Branch:** `feat/dashboard-ui`
**Phase 1 status:** ✅ Complete, end-to-end verified in the browser
**Next phase:** Phase 2 — full data contract (all four JSON files, schema
version check, commit-landed asset check) — see
`docs/plans/dashboard-ui.md` Phase 2 and GitHub issue
[#22](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/22).

## Phase 1 commits on this branch

```
884f172 fix(dashboard): render tracer chart body via pn.bind + datetime axis
7a1e4be fix(dashboard): make app.py self-contained for pyodide build
7453519 feat(dashboard): phase 1 tracer bullet end-to-end pipeline
582a7c9 style(tests): satisfy ruff across pre-existing unit tests
```

## Verification evidence

- `uv run ruff check src/ tests/ scripts/` → clean
- `uv run pytest -m unit --no-cov` → **125 passed, 2 skipped, 0 failed**
- `uv run panel convert src/weather_analytics/dashboard/app.py --to pyodide-worker --out dashboard_build/` → 32KB bundle, builds cleanly
- Browser smoke test against `localhost:8000/app.html` with placeholder
  data → line chart renders with Poppins typography, 5 data points
  (2026-03-01 through 2026-03-05), x-axis datetime, y-axis MWh, no
  console errors, no "could not set initial ranges" warnings

## Pyodide kill-criteria: all clear

| Criterion                               | Result                                                |
| --------------------------------------- | ----------------------------------------------------- |
| Polars wheel loads in Pyodide           | N/A — Phase 1 uses pure-Python data handling          |
| Initial WASM + deps < 25 MB             | 32 KB app bundle; ~18 MB CDN runtime cached after 1st |
| `panel convert` succeeds on minimal app | ✅                                                    |
| Bokeh chart renders with theme applied  | ✅                                                    |

## Six things I learned the hard way (DO NOT LOSE THESE)

These are the non-obvious gotchas that burned hours during Phase 1.
Phase 2+ implementers (including TDD subagents) must know them before
touching the dashboard module.

### 1. `panel convert` runs scripts, not packages

`panel convert --to pyodide-worker` executes the target file as a
standalone script inside Pyodide. It has no knowledge of the parent
`weather_analytics` package and cannot resolve
`from weather_analytics.dashboard.data_loader import ...` — the import
fails with `ModuleNotFoundError` and the convert step aborts with
"file does not publish any Panel contents".

**Phase 1 workaround:** `app.py` is deliberately self-contained and
inlines the palette, Bokeh theme JSON, async fetch helper, and parsing
utilities it needs. `theme.py` and `data_loader.py` still exist in
`src/weather_analytics/dashboard/` and remain the source of truth for
Phase 2+ component code and unit tests — but `app.py` does not import
from them.

**Phase 2 must pick one of these before adding real components:**

- **(a)** `panel convert --requirements theme.py data_loader.py components/filters.py ... app.py` — declare every local module so the worker can import them. Simplest but requires explicit enumeration.
- **(b)** Build-step concatenation — write a preprocessor that inlines the helpers into a bundled `app_bundled.py` before running `panel convert`. More code, but keeps the source modular.
- **(c)** Publish `weather_analytics.dashboard` as a local wheel and install it via `micropip` inside the worker. Heaviest lift, most "proper".

**Do not reintroduce `from weather_analytics.dashboard.* import ...`
inside `app.py` without picking one of these strategies first.** The
current Panel tooling will fail the build.

### 2. `pn.config.theme` is NOT a Bokeh theme

`pn.config.theme` expects one of Panel's design theme name strings
(`"default"`, `"dark"`, etc.) and does a dict lookup. Assigning a
`bokeh.themes.Theme` object raises
`KeyError: <bokeh.themes.theme.Theme object at 0x...>`.

**Correct pattern:**

```python
from bokeh.io import curdoc
from bokeh.themes import Theme

pn.extension(sizing_mode="stretch_width")
curdoc().theme = Theme(json=_THEME_JSON)
```

`curdoc().theme` applies the Bokeh theme to every figure in the current
document, which is what Phase 1 needed.

### 3. Async children of `pn.Column` need `pn.bind`

Passing a bare async function as a `pn.Column` child leaves Panel
treating it as an opaque non-viewable object. The column renders its
other children (like the header) but the async slot stays blank with
no error and no console warning.

**Wrong:**

```python
pn.Column(header, build_body).servable()  # build_body is async def
```

**Right:**

```python
pn.Column(header, pn.bind(build_body)).servable()
```

`pn.bind(fn)` returns a reactive expression that calls `fn()` once on
first render (when there are no dependencies) and handles coroutine
results transparently.

### 4. Bokeh's datetime axis needs `datetime`, not `date`

Bokeh's datetime x-axis internally stores values as milliseconds since
UNIX epoch. It has converters for `datetime.datetime`, `numpy.datetime64`,
and `pandas.Timestamp`. **It does not have a converter for
`datetime.date`.** Passing `date` objects produces multiple
`[bokeh] could not set initial ranges` warnings in the browser console
and the plot body renders as blank space under the title — no line, no
axes, nothing.

**Correct pattern:**

```python
from datetime import datetime

def _parse_iso_date(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)  # returns datetime, not date
    except ValueError:
        return None
```

`datetime.fromisoformat("2026-03-01")` returns a `datetime` at midnight,
which Bokeh can convert to ms-since-epoch cleanly. Use `datetime` end to
end when building data for a Bokeh datetime axis.

### 5. `fig.circle` is deprecated in Bokeh 3.4+

Bokeh 3.4 deprecated the per-glyph shortcut methods (`fig.circle`,
`fig.square`, etc.) in favor of the unified `fig.scatter` method with a
`marker` parameter. The deprecated methods may still work but emit
warnings; in some cases the circles render but the API feels brittle.

**Phase 1 uses `fig.scatter(...)`. Keep that pattern.**

```python
fig.scatter(x=xs, y=ys, size=8, fill_color=_DATA_PRIMARY, line_color=_DATA_PRIMARY)
```

### 6. `uv sync --extra dashboard` (without `--extra dev`) uninstalls dev tools

`uv sync --extra <name>` installs the main deps plus the named extra,
but treats all OTHER extras as "not requested" and REMOVES them if they
were installed. Running `uv sync --extra dashboard` will uninstall
`ruff`, `pytest`, `mypy`, `pytest-cov`, `pytest-mock`, and `pre-commit`
because they live in the `dev` extra.

**Correct invocations:**

```bash
# Daily dev work:
uv sync --all-extras

# Or if you want to be explicit:
uv sync --extra dev --extra dashboard
```

If you ever hit `error: Failed to spawn: ruff` or
`ImportError: cannot import name 'SectionWrapper' from 'iniconfig'`,
this is the likely cause — the environment is in a broken half-state
from a partial install. Recover with:

```bash
uv sync --all-extras --reinstall
```

## Additional protocol / quality rules established during Phase 1

These came up in conversation and should be preserved:

### Don't hide problems behind blanket ruff ignores

When ruff flags issues on your own code, fix the code — don't add
per-file-ignore rules for rules like `PT012`, `PLR2004`, `TRY003`,
`EM101`, `RUF059`. Those usually indicate real code smells (multi-line
`pytest.raises` blocks, magic numbers in assertions, string-literal
exceptions) that are easy to clean up. The only defensible test-level
ignores are the ones that were already in `pyproject.toml` before
Phase 1: `ANN`, `ARG`, `S101`.

Legitimate per-file-ignores added during Phase 1:

- `src/weather_analytics/dashboard/*`: `PLC0415`, `ANN401`, `TRY003`,
  `EM101`, `EM102`, `BLE001` — all justified by the Pyodide compat
  constraint and the user-facing error flow in `app.py`.
- `scripts/*`: `T201`, `INP001` — CI utility scripts use `print()` for
  user-visible log output.

### Pre-existing bugs are in scope

Pre-existing ruff errors in unrelated files (e.g.,
`test_polars_utils_lazy.py`, `test_dlt_resource.py`) were fixed as part
of this branch even though they predate the dashboard work. Rule:
"Just because it's not your work doesn't mean it shouldn't be taken
care of." Keep the tree lint-clean when you can.

### Two-commit split when mixing cleanup with feature

This branch has a `style(tests):` cleanup commit separate from the
`feat(dashboard):` feature commit so reviewers can see what was
cleanup vs. what was the actual feature. Follow this pattern when
touching unrelated files during future phases.

### The formatter auto-strips unused imports

A `ruff check --fix` pass runs after every edit. If you add an `import`
line and the symbol isn't referenced yet, the formatter will silently
remove it before the next edit. **Add imports and their usages in a
single edit**, or the unused import gets yanked and you waste a round
trip re-adding it.

## Deferred manual tasks (not blocking Phase 2 development)

These are needed for the first live production deploy but do NOT need
to happen before Phase 2 TDD work can start. They're listed here so
they're not forgotten.

- [ ] Create GitHub fine-grained PAT scoped to `contents:write` on the
      portfolio repo
- [ ] Store PAT as `WAGA_PORTFOLIO_REPO_TOKEN` in:
  - Dagster Cloud secrets (for the Dagster publish asset)
  - WAGA repo GitHub Actions secrets (for the build workflow)
  - Local `.env` (already has a blank entry pointing at the placeholder)
- [ ] Create empty `/dashboard/` directory in `charleslikesdata` repo
      with a placeholder `index.html`
- [ ] Manually run `waga_dashboard_export_build` and
      `waga_dashboard_export_publish` against real Snowflake data to
      verify the Dagster → GitHub push flow
- [ ] Verify the `build-dashboard.yml` GitHub Action runs and pushes
      the Panel bundle to the portfolio repo
- [ ] Add a "Dashboard" nav link to `charleslikesdata.com` (LAST step
      — only after the working dashboard is already deployed)

## What Phase 2 should look like

Phase 2 expands the export asset to write all four JSON files (manifest,
assets, daily_performance, weather_performance) with the full 15-column
projections specified in `docs/superpowers/specs/2026-04-11-dashboard-ui-design.md`,
adds the schema-version warning banner to the Panel app, and adds the
`waga_dashboard_export_commit_landed` asset check.

**Dispatch strategy (per the dev-cycle skill):** Phase 2+ should use
TDD subagents dispatched one per GitHub issue, with code review between
each subagent dispatch. Phase 1 was implemented directly by the main
agent (per explicit user instruction) because it contained the Pyodide
feasibility gate; subsequent phases are lower-risk and benefit from the
subagent-driven TDD workflow.

Before Phase 2's first subagent is dispatched:

1. Decide how to handle multi-file imports for the dashboard module
   (see learning #1 above — pick option a, b, or c).
2. Confirm the Pyodide kill criteria have NOT regressed (re-run the
   browser smoke test with placeholder data if anything upstream
   changed).

## Key files

- `docs/plans/dashboard-ui.md` — full phased implementation plan, 33
  rigor amendments from CEO review
- `docs/superpowers/specs/2026-04-11-dashboard-ui-design.md` — design
  spec (PRD equivalent)
- `docs/dev-cycle/dashboard-ui.state.md` — dev-cycle state, tracks
  all issues and phase transitions
- GitHub Issues:
  [#20 PRD](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/20),
  [#21 Phase 1](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/21) (done),
  [#22 Phase 2](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/22),
  [#23 Phase 3](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/23),
  [#24 Phase 4](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/24),
  [#25 Phase 5](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/25),
  [#26 Phase 6](https://github.com/cdcoonce/Weather-Adjusted-Generation-Analytics/issues/26)
