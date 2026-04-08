"""dlt ingestion resource for Dagster.

Wraps dlt pipeline creation with Snowflake destination configuration,
using the same WAGA_ credential namespace as WAGASnowflakeResource.
"""

from __future__ import annotations

import base64

import dlt
from dagster import ConfigurableResource


class DltIngestionResource(ConfigurableResource):
    """Dagster resource that creates configured dlt pipelines targeting Snowflake.

    Parameters
    ----------
    pipeline_name : str
        Name for the dlt pipeline (used for state tracking).
    dataset_name : str
        Target Snowflake schema (e.g. ``RAW``).
    snowflake_account : str
        Snowflake account identifier.
    snowflake_user : str
        Snowflake service-account username.
    snowflake_private_key_base64 : str
        Base64-encoded PEM private key (PKCS#8, no passphrase).
    snowflake_warehouse : str
        Snowflake compute warehouse.
    snowflake_database : str
        Target Snowflake database (``WAGA``).
    snowflake_role : str
        Snowflake role with appropriate grants.
    """

    pipeline_name: str
    dataset_name: str
    snowflake_account: str
    snowflake_user: str
    snowflake_private_key_base64: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_role: str

    def _get_private_key_bytes(self) -> bytes:
        """Decode the base64-encoded private key to raw PEM bytes.

        Returns
        -------
        bytes
            PEM-encoded private key bytes.
        """
        return base64.b64decode(self.snowflake_private_key_base64)

    def create_pipeline(
        self,
        pipelines_dir: str | None = None,
    ) -> dlt.Pipeline:
        """Create a configured dlt pipeline with Snowflake destination.

        Parameters
        ----------
        pipelines_dir : str | None
            Override directory for dlt pipeline local state.
            Useful for testing.

        Returns
        -------
        dlt.Pipeline
            A dlt pipeline ready for ``.run()``.
        """
        pem_str = self._get_private_key_bytes().decode("utf-8")

        destination = dlt.destinations.snowflake(
            credentials={
                "host": self.snowflake_account,
                "username": self.snowflake_user,
                "private_key": pem_str,
                "database": self.snowflake_database,
                "warehouse": self.snowflake_warehouse,
                "role": self.snowflake_role,
            },
        )

        kwargs: dict[str, object] = {
            "pipeline_name": self.pipeline_name,
            "destination": destination,
            "dataset_name": self.dataset_name,
        }
        if pipelines_dir is not None:
            kwargs["pipelines_dir"] = pipelines_dir

        return dlt.pipeline(**kwargs)  # type: ignore[arg-type]
