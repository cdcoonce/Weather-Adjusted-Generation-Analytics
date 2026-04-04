"""Snowflake resource with key-pair authentication for Dagster.

This is the single auth path for all Snowflake access in WAGA.
dlt ingestion, dbt transformations, and Polars analytics assets
all use this resource (or its connection) in later phases.
"""

from __future__ import annotations

import base64

from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)
from dagster import ConfigurableResource
from snowflake.connector import SnowflakeConnection
from snowflake.connector import connect as sf_connect


class WAGASnowflakeResource(ConfigurableResource):
    """Dagster resource providing Snowflake connections via key-pair auth.

    All config values should be supplied through ``EnvVar("WAGA_*")`` in the
    Definitions object — never hardcoded.

    Parameters
    ----------
    account : str
        Snowflake account identifier (e.g. ``xy12345.us-east-1``).
    user : str
        Snowflake service-account username.
    private_key_base64 : str
        Base64-encoded PEM private key (PKCS#8, no passphrase).
    warehouse : str
        Default compute warehouse.
    database : str
        Target database (``WAGA``).
    role : str
        Snowflake role with appropriate grants.
    """

    account: str
    user: str
    private_key_base64: str
    warehouse: str
    database: str
    role: str

    def get_connection(self) -> SnowflakeConnection:
        """Create and return a Snowflake connection using key-pair auth.

        Returns
        -------
        snowflake.connector.SnowflakeConnection
            An authenticated Snowflake connection.

        Raises
        ------
        ValueError
            If the base64-encoded key cannot be decoded or parsed.
        """
        pem_bytes = base64.b64decode(self.private_key_base64)
        private_key = load_pem_private_key(pem_bytes, password=None)
        pk_der = private_key.private_bytes(
            encoding=Encoding.DER,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )

        return sf_connect(
            account=self.account,
            user=self.user,
            private_key=pk_der,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
        )
