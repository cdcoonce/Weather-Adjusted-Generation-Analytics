"""Phase 2 fixture validation tests.

These tests ensure the shared fixtures behave as expected and remain
stable building blocks for later phases.

"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
@pytest.mark.io
def test_parquet_roundtrip_paths_exist(
    weather_parquet_path: Path,
    generation_parquet_path: Path,
) -> None:
    """Verify parquet writers create files in temp directories."""
    assert weather_parquet_path.exists()
    assert generation_parquet_path.exists()
