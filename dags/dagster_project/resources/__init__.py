"""Dagster resources for renewable energy pipeline."""

import sys
from pathlib import Path

import duckdb
from dagster import ConfigurableResource

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from src.config import config


class DuckDBResource(ConfigurableResource):
    """
    DuckDB database resource for Dagster.

    Provides connection management for DuckDB database operations
    within Dagster assets and ops.

    Attributes
    ----------
    database_path : str
        Path to the DuckDB database file

    """

    database_path: str = str(config.duckdb_path)

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Get a DuckDB connection.

        Returns
        -------
        duckdb.DuckDBPyConnection
            Active database connection

        """
        return duckdb.connect(self.database_path)

    def execute_query(self, query: str) -> list:
        """
        Execute a query and return results.

        Parameters
        ----------
        query : str
            SQL query to execute

        Returns
        -------
        list
            Query results

        """
        conn = self.get_connection()
        try:
            result = conn.execute(query).fetchall()
            return result
        finally:
            conn.close()


class DltResource(ConfigurableResource):
    """
    dlt pipeline resource for Dagster.

    Provides access to dlt pipeline functionality for data ingestion.

    Attributes
    ----------
    pipeline_name : str
        Name of the dlt pipeline
    destination : str
        Destination type (e.g., 'duckdb')
    dataset_name : str
        Dataset/schema name for ingestion

    """

    pipeline_name: str = config.dlt_pipeline_name
    destination: str = config.dlt_destination
    dataset_name: str = config.dlt_schema

    def get_pipeline_config(self) -> dict:
        """
        Get pipeline configuration.

        Returns
        -------
        dict
            Pipeline configuration dictionary

        """
        return {
            "pipeline_name": self.pipeline_name,
            "destination": self.destination,
            "dataset_name": self.dataset_name,
        }


# Resource instances
duckdb_resource = DuckDBResource()
dlt_resource = DltResource()
