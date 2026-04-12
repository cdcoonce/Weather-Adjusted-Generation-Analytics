"""Async JSON data loaders for the Panel dashboard.

Uses ``pyodide.http.pyfetch`` in the browser and falls back to a stdlib
``urllib`` read when running outside Pyodide (e.g., local tests, local
``panel serve`` smoke runs). The fallback is intentional and kept tiny
so tests can exercise the parsing and schema handling without needing
a browser runtime.

Important: do NOT use ``requests`` here. ``requests`` relies on sockets
that the Pyodide sandbox does not expose, so the browser build would
break at import time.
"""

import json
import sys
from dataclasses import dataclass
from typing import Any

import polars as pl

# Resolved lazily so importing this module doesn't hit the network.
_CACHE: dict[str, Any] = {}

_HTTP_ERROR_FLOOR = 400

EXPECTED_SCHEMA_VERSION = "1.0"

# Relative data directory served by GitHub Pages alongside the compiled
# Panel app. Lives at ``charleslikesdata.com/dashboard/data/*.json``.
DEFAULT_DATA_BASE = "./data"


@dataclass(frozen=True)
class Manifest:
    """Dashboard export manifest metadata.

    Parsed from ``manifest.json``. ``schema_version`` is compared against
    ``EXPECTED_SCHEMA_VERSION`` at app startup; mismatch triggers a
    non-blocking warning banner in the UI.
    """

    generated_at: str
    pipeline_run_id: str
    date_range_start: str
    date_range_end: str
    asset_count: int
    row_counts: dict[str, int]
    schema_version: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Manifest":
        """Construct a ``Manifest`` from a parsed JSON dict.

        Missing optional fields default to empty values; missing required
        fields raise ``KeyError`` so the app can surface a clear error.
        """
        return cls(
            generated_at=data["generated_at"],
            pipeline_run_id=data.get("pipeline_run_id", ""),
            date_range_start=data["date_range"]["start"],
            date_range_end=data["date_range"]["end"],
            asset_count=int(data["asset_count"]),
            row_counts=dict(data.get("row_counts", {})),
            schema_version=data.get("schema_version", ""),
        )

    @property
    def schema_matches(self) -> bool:
        """Return True if the exported schema matches the app's expectation."""
        return self.schema_version == EXPECTED_SCHEMA_VERSION


def clear_cache() -> None:
    """Drop all cached payloads. Useful in tests."""
    _CACHE.clear()


async def _fetch_text(url: str) -> str:
    """Fetch a URL and return the response body as text.

    In Pyodide, uses ``pyodide.http.pyfetch`` (async). Outside Pyodide,
    falls back to ``urllib.request.urlopen`` synchronously.

    Parameters
    ----------
    url : str
        Absolute or relative URL to fetch.

    Returns
    -------
    str
        Response body.

    Raises
    ------
    RuntimeError
        If the fetch fails. The caller translates this into an error banner.
    """
    if "pyodide" in sys.modules:
        try:
            from pyodide.http import pyfetch  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "pyodide is imported but pyodide.http.pyfetch is unavailable"
            ) from exc
        response = await pyfetch(url)
        if response.status >= _HTTP_ERROR_FLOOR:
            raise RuntimeError(f"Fetch of {url} failed with HTTP {response.status}")
        return await response.string()

    # Non-Pyodide fallback (tests, local dev): synchronous urllib.
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(url) as response:
            return str(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Fetch of {url} failed: {exc}") from exc


async def load_json(name: str, base: str = DEFAULT_DATA_BASE) -> Any:
    """Fetch and parse a JSON file from the dashboard data directory.

    Results are cached by ``name``; subsequent calls return the cached
    payload without a network round trip. Call ``clear_cache()`` in tests.

    Parameters
    ----------
    name : str
        File name (e.g., ``daily_performance.json``).
    base : str, optional
        Base URL/path. Defaults to ``./data`` (relative to the served
        Panel app).

    Returns
    -------
    Any
        Parsed JSON structure (list or dict).

    Raises
    ------
    RuntimeError
        If the fetch fails.
    json.JSONDecodeError
        If the response body is not valid JSON.
    """
    cache_key = f"{base}/{name}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    payload = await _fetch_text(cache_key)
    parsed = json.loads(payload)
    _CACHE[cache_key] = parsed
    return parsed


async def load_manifest(base: str = DEFAULT_DATA_BASE) -> Manifest:
    """Load and parse ``manifest.json``."""
    raw = await load_json("manifest.json", base=base)
    return Manifest.from_dict(raw)


async def load_daily_performance(base: str = DEFAULT_DATA_BASE) -> pl.DataFrame:
    """Load ``daily_performance.json`` as a Polars DataFrame."""
    raw = await load_json("daily_performance.json", base=base)
    return pl.DataFrame(raw)
