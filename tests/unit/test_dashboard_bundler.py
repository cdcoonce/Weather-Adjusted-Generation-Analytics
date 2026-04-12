"""Unit tests for ``scripts.build_dashboard_app`` bundler logic."""


# The bundler lives in scripts/, which is not a package (no __init__.py).
# We import the pure helper functions directly using importlib so we don't
# need to add scripts/ to sys.path or change the test runner config.
import importlib.util
import sys
from pathlib import Path

import pytest

_BUNDLER_PATH = (
    Path(__file__).parent.parent.parent / "scripts" / "build_dashboard_app.py"
)


def _load_bundler() -> object:
    """Dynamically load build_dashboard_app as a module."""
    spec = importlib.util.spec_from_file_location("build_dashboard_app", _BUNDLER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["build_dashboard_app"] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


# ===========================================================================
# Pure helper: strip_package_imports
# ===========================================================================


@pytest.mark.unit
def test_bundle_strips_package_imports() -> None:
    """Lines matching ``from weather_analytics.dashboard.`` are removed."""
    bundler = _load_bundler()
    source = (
        "import os\n"
        "from weather_analytics.dashboard.theme import build_theme_json\n"
        "x = 1\n"
    )
    result = bundler.strip_package_imports(source)  # type: ignore[attr-defined]
    assert "from weather_analytics.dashboard." not in result
    assert "import os" in result
    assert "x = 1" in result


@pytest.mark.unit
def test_bundle_strips_only_matching_imports() -> None:
    """Unrelated imports and code are preserved."""
    bundler = _load_bundler()
    source = (
        "from weather_analytics.other_package import foo\n"
        "from weather_analytics.dashboard.theme import X\n"
        "import json\n"
    )
    result = bundler.strip_package_imports(source)  # type: ignore[attr-defined]
    # dashboard import removed
    assert "from weather_analytics.dashboard." not in result
    # other import preserved (not a dashboard sub-import)
    assert "from weather_analytics.other_package import foo" in result
    assert "import json" in result


# ===========================================================================
# Pure helper: concatenate_modules
# ===========================================================================


@pytest.mark.unit
def test_bundle_strips_multiline_package_imports() -> None:
    """Multi-line parenthesised imports from dashboard packages are fully removed."""
    bundler = _load_bundler()
    source = (
        "import os\n"
        "from weather_analytics.dashboard.data_loader import (\n"
        "    EXPECTED_SCHEMA_VERSION,\n"
        "    load_daily_performance,\n"
        "    load_manifest,\n"
        ")\n"
        "x = 1\n"
    )
    result = bundler.strip_package_imports(source)  # type: ignore[attr-defined]
    assert "from weather_analytics.dashboard." not in result
    assert "EXPECTED_SCHEMA_VERSION" not in result
    assert "load_daily_performance" not in result
    # Closing paren of the import block must not be left as a stray line
    assert result.strip().endswith("x = 1")
    assert "import os" in result
    assert "x = 1" in result


@pytest.mark.unit
def test_bundle_contains_all_module_sources() -> None:
    """Given two fake module sources, both appear in the bundle output."""
    bundler = _load_bundler()
    modules = [
        ("theme.py", "THEME_CONTENT = True\n"),
        ("data_loader.py", "LOADER_CONTENT = True\n"),
    ]
    result = bundler.concatenate_modules(modules)  # type: ignore[attr-defined]
    assert "THEME_CONTENT = True" in result
    assert "LOADER_CONTENT = True" in result


@pytest.mark.unit
def test_bundle_adds_section_banners() -> None:
    """Bundle output contains BEGIN markers for each module."""
    bundler = _load_bundler()
    modules = [
        ("theme.py", "x = 1\n"),
        ("data_loader.py", "y = 2\n"),
    ]
    result = bundler.concatenate_modules(modules)  # type: ignore[attr-defined]
    assert "BEGIN theme.py" in result
    assert "BEGIN data_loader.py" in result
