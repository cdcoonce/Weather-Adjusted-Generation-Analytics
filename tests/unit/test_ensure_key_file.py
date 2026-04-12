"""Unit tests for _ensure_key_file() in dbt_assets.py."""

from __future__ import annotations

import base64
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

from weather_analytics.assets.dbt_assets import _ensure_key_file

# Generate a real (small) private key so load_pem_private_key succeeds.
_TEST_KEY = Ed25519PrivateKey.generate()
_TEST_PEM = _TEST_KEY.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
_FAKE_B64 = base64.b64encode(_TEST_PEM).decode()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Ensure key-related env vars are cleaned up after each test.

    _ensure_key_file() writes to os.environ directly, so we need to
    capture and restore the state ourselves.
    """
    orig_path = os.environ.pop("WAGA_SNOWFLAKE_PRIVATE_KEY_PATH", None)
    orig_b64 = os.environ.pop("WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64", None)
    yield
    # Restore original state (or remove if not originally set)
    if orig_path is not None:
        os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_PATH"] = orig_path
    else:
        os.environ.pop("WAGA_SNOWFLAKE_PRIVATE_KEY_PATH", None)
    if orig_b64 is not None:
        os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64"] = orig_b64
    else:
        os.environ.pop("WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64", None)


@pytest.mark.unit
def test_creates_file_and_sets_env_var() -> None:
    """_ensure_key_file decodes base64 to a .p8 file and sets the path env var."""
    os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64"] = _FAKE_B64

    _ensure_key_file()

    path = os.environ.get("WAGA_SNOWFLAKE_PRIVATE_KEY_PATH", "")
    assert path, "WAGA_SNOWFLAKE_PRIVATE_KEY_PATH should be set"
    assert Path(path).exists(), "Temp .p8 file should exist"
    assert Path(path).suffix == ".p8"
    content = Path(path).read_bytes()
    assert content.startswith(b"-----BEGIN PRIVATE KEY-----")
    assert content.endswith(b"-----END PRIVATE KEY-----\n")


@pytest.mark.unit
def test_file_has_restrictive_permissions() -> None:
    """The temp .p8 file should have 0o600 permissions."""
    os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64"] = _FAKE_B64

    _ensure_key_file()

    path = os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_PATH"]
    mode = oct(Path(path).stat().st_mode & 0o777)
    assert mode == "0o600"


@pytest.mark.unit
def test_idempotent_when_path_already_set(tmp_path: Path) -> None:
    """Skip work when WAGA_SNOWFLAKE_PRIVATE_KEY_PATH points to a file."""
    existing_file = tmp_path / "existing.p8"
    existing_file.write_text("existing-key")
    os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_PATH"] = str(existing_file)
    os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64"] = _FAKE_B64

    _ensure_key_file()

    assert os.environ["WAGA_SNOWFLAKE_PRIVATE_KEY_PATH"] == str(existing_file)
    assert existing_file.read_text() == "existing-key"


@pytest.mark.unit
def test_noop_when_no_base64_env_var() -> None:
    """No-op when WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64 is unset."""
    _ensure_key_file()

    assert not os.environ.get("WAGA_SNOWFLAKE_PRIVATE_KEY_PATH")
