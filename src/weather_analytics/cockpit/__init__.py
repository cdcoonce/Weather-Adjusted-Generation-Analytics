"""Standalone static dashboard for WAGA (afk-cockpit style).

Reads the 4 JSON exports written by the ``waga_dashboard_export_build`` Dagster
asset and renders a single self-contained ``dist/index.html`` deployed to
Cloudflare Pages. No Dagster context, no Snowflake, no chart library.
"""

__version__ = "0.1.0"
