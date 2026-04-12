"""Build script: bundle dashboard modules and invoke panel convert.

Concatenates local dashboard modules into ``dashboard_build/app_bundled.py``
so that ``panel convert`` can process a single self-contained file without
needing ``weather_analytics.dashboard`` on ``sys.path`` inside Pyodide.

Algorithm
---------
1. Read each module listed in ``MODULES_TO_INLINE`` in order.
2. Strip lines that match ``from weather_analytics.dashboard.`` — those
   imports resolve by inlining.
3. Write the concatenated source to ``dashboard_build/app_bundled.py`` with
   section banners like ``# === BEGIN theme.py ===``.
4. Read ``src/weather_analytics/dashboard/app.py``, strip its own
   ``from weather_analytics.dashboard.`` imports (now inlined), and append.
5. Run ``panel convert dashboard_build/app_bundled.py --to pyodide-worker
   --out dashboard_build/``.

Usage
-----
    uv run python scripts/build_dashboard_app.py
"""

import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Module ordering matters: each module may depend on the one before it.
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).parent.parent
MODULES_TO_INLINE: list[Path] = [
    _ROOT / "src" / "weather_analytics" / "dashboard" / "theme.py",
    _ROOT / "src" / "weather_analytics" / "dashboard" / "data_loader.py",
    _ROOT / "src" / "weather_analytics" / "dashboard" / "components" / "filters.py",
    _ROOT / "src" / "weather_analytics" / "dashboard" / "components" / "kpi_cards.py",
    _ROOT
    / "src"
    / "weather_analytics"
    / "dashboard"
    / "components"
    / "_chart_helpers.py",
    _ROOT / "src" / "weather_analytics" / "dashboard" / "components" / "fleet_view.py",
    _ROOT / "src" / "weather_analytics" / "dashboard" / "components" / "asset_view.py",
]
_APP_PY = _ROOT / "src" / "weather_analytics" / "dashboard" / "app.py"
_BUILD_DIR = _ROOT / "dashboard_build"
_BUNDLE_OUT = _BUILD_DIR / "app_bundled.py"

_STRIP_PREFIX = "from weather_analytics.dashboard."


# ===========================================================================
# Pure helper functions (tested in tests/unit/test_dashboard_bundler.py)
# ===========================================================================


def strip_package_imports(source: str) -> str:
    """Remove intra-dashboard import lines from source text.

    Any line whose stripped form starts with ``from weather_analytics.dashboard.``
    is dropped, along with any continuation lines that form part of a
    parenthesised import block (i.e., lines up to and including the closing
    ``)``) that immediately follow such a line.

    All other lines (including other ``from weather_analytics.*`` imports) are
    preserved.

    Parameters
    ----------
    source : str
        Raw Python source text.

    Returns
    -------
    str
        Source text with matching import blocks removed.
    """
    filtered: list[str] = []
    inside_block = False
    for line in source.splitlines(keepends=True):
        if inside_block:
            # Skip continuation lines until the closing paren is consumed.
            if line.rstrip().endswith(")") or line.strip() == ")":
                inside_block = False
            continue
        stripped = line.strip()
        if stripped.startswith(_STRIP_PREFIX):
            # Check whether this is a multi-line import (opening paren present
            # but no closing paren on the same line).
            if "(" in stripped and ")" not in stripped:
                inside_block = True
            # Either way, skip this opening line.
            continue
        filtered.append(line)
    return "".join(filtered)


def concatenate_modules(modules: list[tuple[str, str]]) -> str:
    """Concatenate named module source texts with section banners.

    Parameters
    ----------
    modules : list[tuple[str, str]]
        Pairs of ``(filename, source_text)``.

    Returns
    -------
    str
        Single source string with ``# === BEGIN <name> ===`` banners between
        each module's content.
    """
    parts: list[str] = []
    for name, source in modules:
        parts.append(f"# === BEGIN {name} ===\n")
        parts.append(source)
        parts.append("\n")
    return "".join(parts)


# ===========================================================================
# Build orchestration
# ===========================================================================


def _build_bundle() -> Path:
    """Read modules, strip intra-dashboard imports, write bundle file."""
    module_sources: list[tuple[str, str]] = []
    for module_path in MODULES_TO_INLINE:
        raw = module_path.read_text(encoding="utf-8")
        stripped = strip_package_imports(raw)
        module_sources.append((module_path.name, stripped))

    # Append app.py with its dashboard imports stripped (they are now inlined).
    app_raw = _APP_PY.read_text(encoding="utf-8")
    app_stripped = strip_package_imports(app_raw)
    module_sources.append((_APP_PY.name, app_stripped))

    bundle_text = concatenate_modules(module_sources)

    _BUILD_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[build] Writing bundle to {_BUNDLE_OUT}")
    _BUNDLE_OUT.write_text(bundle_text, encoding="utf-8")
    return _BUNDLE_OUT


def _run_panel_convert(bundle_path: Path) -> None:
    """Invoke ``panel convert`` on the bundle file."""
    cmd = [
        sys.executable,
        "-m",
        "panel",
        "convert",
        str(bundle_path),
        "--to",
        "pyodide-worker",
        "--out",
        str(_BUILD_DIR),
    ]
    # stdout/stderr inherit from parent process (capture_output=False), so
    # text=True has no effect here.  Panel also auto-detects ``import polars``
    # in the bundle and adds it to the Pyodide requirements list automatically.
    result = subprocess.run(cmd, check=False, capture_output=False)
    if result.returncode != 0:
        print(
            f"[build] panel convert failed (exit {result.returncode})", file=sys.stderr
        )
        sys.exit(result.returncode)
    print("[build] panel convert succeeded")


if __name__ == "__main__":
    bundle = _build_bundle()
    _run_panel_convert(bundle)
