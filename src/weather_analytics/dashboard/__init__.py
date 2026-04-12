"""WAGA interactive dashboard (Panel + Bokeh compiled to Pyodide/WASM).

This package contains the Panel app that renders the Weather-Adjusted
Generation Analytics dashboard on charleslikesdata.com/dashboard/. The
code runs entirely in the visitor's browser via Pyodide — there is no
backend server.

Architecture
------------
- ``app.py`` — entry point, assembles the Panel layout, applies the Bokeh
  theme inside ``servable()`` (not at import time, to avoid test pollution).
- ``data_loader.py`` — async JSON loaders using ``pyodide.http.pyfetch``.
  Each function is awaited by components that need it. Caches results
  in-memory so tab switches do not refetch.
- ``theme.py`` — exports a Bokeh ``Theme`` object. Never applied at
  import time; ``app.py`` applies it inside the servable function.
- ``static/portfolio.css`` — imports Poppins and declares palette CSS
  variables matching charleslikesdata.com.
- ``components/`` — per-tab view modules added incrementally in later
  dev-cycle phases. Phase 1 has a minimal single-chart app only.

Deployment
----------
1. Local: ``uv run panel convert src/weather_analytics/dashboard/app.py
   --to pyodide-worker --out dashboard_build/``
2. CI: ``.github/workflows/build-dashboard.yml`` runs the same command
   on every push touching ``src/weather_analytics/dashboard/**`` and
   pushes the build output to the portfolio repo.

Dependency constraints
----------------------
Only Pyodide-compatible packages may be imported from this module:
``panel``, ``bokeh``, ``polars``, ``numpy``. Importing ``requests`` or
any C-extension package not in the Pyodide distribution will break the
browser build.

Pyodide CDN risk
----------------
Pyodide is loaded from ``cdn.jsdelivr.net`` (Panel's default). A
compromised CDN could inject malicious Python into visitor browsers.
Accepted risk for a personal portfolio project. Mitigation is to
monitor the deploy pipeline for unexpected changes.

Verified version combination (Phase 1)
--------------------------------------
- panel >= 1.4, < 1.5
- bokeh >= 3.4, < 3.5
- polars >= 0.19 (Pyodide wheel availability verified at build time)
- Pyodide 0.25+ (CDN default as of Panel 1.4)

Do not upgrade these without re-running ``panel convert`` smoke test.
"""
