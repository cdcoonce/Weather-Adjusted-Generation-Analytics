"""Unit tests for the custom SnowflakeResource with key-pair auth."""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

from weather_analytics.resources.snowflake import WAGASnowflakeResource


def _generate_test_pem_b64() -> str:
    """Generate a valid RSA private key PEM and return base64-encoded string."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    pem_bytes = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    return base64.b64encode(pem_bytes).decode()


_TEST_PEM_B64 = _generate_test_pem_b64()


@pytest.mark.unit
class TestWAGASnowflakeResourceConfig:
    """Verify resource accepts config and validates fields."""

    def test_instantiates_with_all_fields(self) -> None:
        """Resource can be instantiated with all required fields."""
        resource = WAGASnowflakeResource(
            account="test_account",
            user="test_user",
            private_key_base64=_TEST_PEM_B64,
            warehouse="test_wh",
            database="test_db",
            role="test_role",
        )
        assert resource.account == "test_account"
        assert resource.user == "test_user"
        assert resource.warehouse == "test_wh"
        assert resource.database == "test_db"
        assert resource.role == "test_role"

    def test_private_key_base64_stored(self) -> None:
        """Private key base64 is stored on the resource."""
        resource = WAGASnowflakeResource(
            account="acct",
            user="usr",
            private_key_base64=_TEST_PEM_B64,
            warehouse="wh",
            database="db",
            role="role",
        )
        assert resource.private_key_base64 == _TEST_PEM_B64


@pytest.mark.unit
class TestWAGASnowflakeResourceGetConnection:
    """Verify get_connection() decodes key and calls connector."""

    @patch("weather_analytics.resources.snowflake.sf_connect")
    def test_get_connection_calls_connector(
        self,
        mock_connect: MagicMock,
    ) -> None:
        """get_connection() calls snowflake.connector.connect with correct args."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        resource = WAGASnowflakeResource(
            account="myaccount",
            user="myuser",
            private_key_base64=_TEST_PEM_B64,
            warehouse="mywh",
            database="mydb",
            role="myrole",
        )

        conn = resource.get_connection()

        assert conn is mock_conn
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["account"] == "myaccount"
        assert call_kwargs["user"] == "myuser"
        assert call_kwargs["warehouse"] == "mywh"
        assert call_kwargs["database"] == "mydb"
        assert call_kwargs["role"] == "myrole"
        assert "private_key" in call_kwargs
        # private_key should be DER-encoded bytes
        assert isinstance(call_kwargs["private_key"], bytes)

    @patch("weather_analytics.resources.snowflake.sf_connect")
    def test_get_connection_decodes_base64_and_loads_pem(
        self,
        mock_connect: MagicMock,
    ) -> None:
        """get_connection() base64-decodes and deserializes the PEM key."""
        mock_connect.return_value = MagicMock()

        resource = WAGASnowflakeResource(
            account="acct",
            user="usr",
            private_key_base64=_TEST_PEM_B64,
            warehouse="wh",
            database="db",
            role="role",
        )

        resource.get_connection()

        call_kwargs = mock_connect.call_args.kwargs
        pk_bytes = call_kwargs["private_key"]
        # DER-encoded private keys start with ASN.1 SEQUENCE tag (0x30)
        assert pk_bytes[0:1] == b"\x30"

    @patch("weather_analytics.resources.snowflake.sf_connect")
    def test_get_connection_returns_connection_object(
        self,
        mock_connect: MagicMock,
    ) -> None:
        """get_connection() returns whatever snowflake.connector.connect returns."""
        sentinel = object()
        mock_connect.return_value = sentinel

        resource = WAGASnowflakeResource(
            account="acct",
            user="usr",
            private_key_base64=_TEST_PEM_B64,
            warehouse="wh",
            database="db",
            role="role",
        )

        result = resource.get_connection()
        assert result is sentinel
