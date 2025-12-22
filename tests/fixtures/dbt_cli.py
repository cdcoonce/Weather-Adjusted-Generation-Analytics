"""Helpers for running dbt in integration tests.

These helpers intentionally run dbt as a subprocess to emulate real usage.
We keep all dbt artifacts (profiles/target) under pytest-provided temp dirs
so tests never write to the repository.

"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DbtInvocation:
    """Configuration for a dbt CLI invocation."""

    project_dir: Path
    profiles_dir: Path
    target_path: Path
    target: str = "dev"


def find_dbt_executable() -> str | None:
    """Return the dbt executable path, if available."""

    return shutil.which("dbt")


def write_temp_profiles_yml(*, profiles_dir: Path, duckdb_path: Path) -> Path:
    """Write a minimal DuckDB dbt profile pointing at `duckdb_path`.

    Parameters
    ----------
    profiles_dir : Path
        Directory to write the `profiles.yml` into.
    duckdb_path : Path
        DuckDB database path for the dbt target.

    Returns
    -------
    Path
        Path to the written `profiles.yml`.

    """

    profiles_dir.mkdir(parents=True, exist_ok=True)

    content = (
        "renewable_dbt:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: {duckdb_path}\n"
        "      schema: renewable_energy\n"
        "      threads: 1\n"
        "      extensions:\n"
        "        - parquet\n"
        "        - httpfs\n"
    )

    path = profiles_dir / "profiles.yml"
    path.write_text(content, encoding="utf-8")
    return path


def run_dbt(
    *args: str,
    invocation: DbtInvocation,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run dbt as a subprocess.

    Raises a RuntimeError with captured stdout/stderr on failure.

    Parameters
    ----------
    *args : str
        Arguments to pass to dbt (e.g., "build", "--select", "staging.stg_weather").
    invocation : DbtInvocation
        dbt invocation configuration.
    extra_env : dict[str, str] | None, default=None
        Extra environment variables.

    Returns
    -------
    subprocess.CompletedProcess[str]
        Completed process result.

    """

    dbt = find_dbt_executable()
    if dbt is None:
        msg = "dbt executable not found on PATH"
        raise RuntimeError(msg)

    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    cmd = [
        dbt,
        *args,
        "--project-dir",
        str(invocation.project_dir),
        "--profiles-dir",
        str(invocation.profiles_dir),
        "--target",
        invocation.target,
        "--target-path",
        str(invocation.target_path),
    ]

    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )

    if result.returncode != 0:
        msg = (
            "dbt command failed\n"
            f"cmd: {' '.join(cmd)}\n"
            f"returncode: {result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}\n"
        )
        raise RuntimeError(msg)

    return result
