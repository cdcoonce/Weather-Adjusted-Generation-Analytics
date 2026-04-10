"""Dagster @dbt_assets wrapper for the renewable_dbt project.

Exposes all dbt models as Dagster software-defined assets so they
participate in the WAGA asset graph, lineage, and scheduling.

The manifest is generated at build time (``dbt parse`` in the Dagster
Cloud deploy workflow).  Locally, ``DbtProject.prepare_if_dev()``
handles manifest generation when running ``dagster dev``.
"""

import atexit
import base64
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from dagster_dbt import DbtProject

DBT_PROJECT_DIR = "dbt/renewable_dbt"
DBT_PROFILES_DIR = "dbt/renewable_dbt/profiles"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)
dbt_project.prepare_if_dev()


def _ensure_key_file() -> None:
    """Decode base64 PEM env var to a temp .p8 file for dbt-snowflake.

    ``WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64`` stores base64-encoded PEM, but
    dbt-snowflake's ``private_key_path`` needs a file on disk.  Writes
    once per process and sets ``WAGA_SNOWFLAKE_PRIVATE_KEY_PATH`` for
    ``profiles.yml`` to reference.  The temp file is registered for
    cleanup on process exit via ``atexit``.

    Idempotent — skips if ``WAGA_SNOWFLAKE_PRIVATE_KEY_PATH`` is already
    set and the file exists.
    """
    existing_path = os.environ.get("WAGA_SNOWFLAKE_PRIVATE_KEY_PATH", "")
    if existing_path and Path(existing_path).exists():
        return

    b64_key = os.environ.get("WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64", "")
    if not b64_key:
        return

    # Round-trip the key through cryptography to normalize PEM framing.
    # Raw base64 decode may preserve whitespace/newline issues that cause
    # dbt-snowflake's "MalformedFraming" error.
    from cryptography.hazmat.primitives.serialization import (  # noqa: PLC0415
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
    )

    raw_pem = base64.b64decode(b64_key)
    private_key = load_pem_private_key(raw_pem, password=None)
    pem_bytes = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    fd, path = tempfile.mkstemp(suffix=".p8")
    os.write(fd, pem_bytes)
    os.close(fd)
    os.chmod(path, 0o600)
    atexit.register(lambda p: os.unlink(p) if os.path.exists(p) else None, path)
    os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_PATH"] = path


# Build the dbt assets only when a manifest is available.
# In CI unit tests the manifest does not exist; waga_dbt_assets will be
# None and definitions.py filters it out.
waga_dbt_assets: Any = None

if dbt_project.manifest_path.exists():
    from dagster import AssetExecutionContext
    from dagster_dbt import DbtCliResource, dbt_assets

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        name="waga_dbt_assets",
    )
    def waga_dbt_assets(
        context: AssetExecutionContext, dbt: DbtCliResource
    ) -> Iterator:  # type: ignore[no-redef]
        """Materialize all dbt models in the renewable_dbt project.

        Parameters
        ----------
        context : AssetExecutionContext
            Dagster execution context.
        dbt : DbtCliResource
            Dagster-managed dbt CLI resource.
        """
        _ensure_key_file()
        yield from dbt.cli(["build"], context=context).stream()
