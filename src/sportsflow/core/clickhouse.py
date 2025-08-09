import os
import logging
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from typing import Optional, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


class ClickHouseConnector:
    """
    Backend-agnostic ClickHouse connector using the official clickhouse-connect package.
    Automatically detects whether it's running locally or in K3s and uses the appropriate connection.
    """

    def __init__(
        self,
        # Local development settings (via ingress)
        external_host: str = "clickhouse-api.juicebox.com",
        external_port: int = 443,
        external_secure: bool = True,
        external_verify_ssl: bool = False,  # Often self-signed in dev
        # K3s internal settings (via service)
        internal_host: str = "clickhouse-clickhouse-cluster.clickhouse.svc.cluster.local",
        internal_port: int = 8123,
        internal_secure: bool = False,
        internal_verify_ssl: bool = True,
        # Common settings
        username: Optional[str] = "default",
        password: Optional[str] = "default",
        database: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize the ClickHouse connector.

        Args:
            external_host: External hostname for local development
            external_port: External port for local development
            external_secure: Use HTTPS for external connections
            external_verify_ssl: Verify SSL certificates for external (False for self-signed)
            internal_host: Internal K3s service name
            internal_port: Internal service port
            internal_secure: Use HTTPS for internal connections
            internal_verify_ssl: Verify SSL certificates for internal
            username: ClickHouse username (defaults to env var or 'default')
            password: ClickHouse password (defaults to env var)
            database: Database name (defaults to env var or None)
            **kwargs: Additional arguments passed to clickhouse_connect.get_client()
        """
        self.external_host = external_host
        self.external_port = external_port
        self.external_secure = external_secure
        self.external_verify_ssl = external_verify_ssl
        self.internal_host = internal_host
        self.internal_port = internal_port
        self.internal_secure = internal_secure
        self.internal_verify_ssl = internal_verify_ssl

        # Get credentials from env vars or params
        self.username = os.getenv("CLICKHOUSE_USERNAME", None) or username
        self.password = os.getenv("CLICKHOUSE_PASSWORD", None) or password
        self.database = database or os.getenv("CLICKHOUSE_DATABASE", None)

        # Store additional kwargs for client creation
        self.client_kwargs = kwargs

        # Detect environment and create client
        self.is_in_k3s = self._detect_k3s_environment()
        self.client = self._create_client()

        logger.info(
            f"Connected to ClickHouse via {'internal service' if self.is_in_k3s else 'external ingress'}"
        )

    def _detect_k3s_environment(self) -> bool:
        """
        Detect if running inside a K3s cluster.

        Returns:
            True if running in K3s, False otherwise
        """
        # K3s specific indicators
        k3s_indicators = [
            os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token"),
            os.getenv("KUBERNETES_SERVICE_HOST"),
            os.getenv("KUBERNETES_PORT"),
            os.getenv("K3S_NODE_NAME"),  # K3s specific
            os.path.exists("/etc/rancher/k3s"),  # K3s specific
        ]

        return any(k3s_indicators)

    def _create_client(self) -> Client:
        """Create the clickhouse-connect client with appropriate settings."""
        if self.is_in_k3s:
            # Use internal service
            host = self.internal_host
            port = self.internal_port
            secure = self.internal_secure
            verify = self.internal_verify_ssl
        else:
            # Use external ingress
            host = self.external_host
            port = self.external_port
            secure = self.external_secure
            verify = self.external_verify_ssl

        client_config = {
            "host": host,
            "port": port,
            "username": self.username,
            "password": self.password,
            "secure": secure,
            "verify": verify,
            **self.client_kwargs,
        }

        # Only add database if specified
        if self.database:
            client_config["database"] = self.database

        return clickhouse_connect.get_client(**client_config)

    def health_check(self) -> bool:
        """Check if ClickHouse is accessible."""
        try:
            result = self.client.command("SELECT 1")
            return result == 1
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict] = None,
        settings: Optional[Dict] = None,
    ) -> Any:
        """
        Execute a ClickHouse query using the official driver.

        Args:
            query: SQL query to execute
            parameters: Query parameters for binding
            settings: ClickHouse settings for the query

        Returns:
            QueryResult object from clickhouse-connect
        """
        return self.client.query(query, parameters=parameters, settings=settings)

    def query_df(
        self,
        query: str,
        parameters: Optional[Dict] = None,
        settings: Optional[Dict] = None,
    ) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.

        Args:
            query: SQL query to execute
            parameters: Query parameters for binding
            settings: ClickHouse settings for the query

        Returns:
            pandas.DataFrame with query results
        """
        return self.client.query_df(query, parameters=parameters, settings=settings)

    def insert_df(
        self,
        table: str,
        df: pd.DataFrame,
        database: Optional[str] = None,
        settings: Optional[Dict] = None,
    ) -> Dict:
        """
        Insert a pandas DataFrame into ClickHouse.

        Args:
            table: Target table name
            df: DataFrame to insert
            database: Target database (uses client default if not specified)
            settings: ClickHouse settings for the insert

        Returns:
            Insert summary dictionary
        """
        return self.client.insert_df(table, df, database=database, settings=settings)

    def insert_data(
        self,
        table: str,
        data,
        column_names: Optional[list] = None,
        database: Optional[str] = None,
        settings: Optional[Dict] = None,
    ) -> Dict:
        """
        Insert data into ClickHouse table.

        Args:
            table: Target table name
            data: Data to insert (sequence of sequences)
            column_names: Column names (uses '*' if not specified)
            database: Target database
            settings: ClickHouse settings for the insert

        Returns:
            Insert summary dictionary
        """
        return self.client.insert(
            table,
            data,
            column_names=column_names or "*",
            database=database,
            settings=settings,
        )

    def command(
        self,
        cmd: str,
        parameters: Optional[Dict] = None,
        settings: Optional[Dict] = None,
    ) -> Any:
        """
        Execute a ClickHouse command (DDL, single value queries, etc.).

        Args:
            cmd: ClickHouse SQL command
            parameters: Query parameters for binding
            settings: ClickHouse settings for the command

        Returns:
            Command result or summary dictionary
        """
        return self.client.command(cmd, parameters=parameters, settings=settings)

    def get_table_schema(
        self, table: str, database: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get table schema information.

        Args:
            table: Table name
            database: Database name (uses client default if not specified)

        Returns:
            DataFrame with column information
        """
        full_table = f"{database}.{table}" if database else table
        return self.query_df(f"DESCRIBE TABLE {full_table}")

    def close(self):
        """Close the connection."""
        if hasattr(self.client, "close"):
            self.client.close()


if __name__ == "__main__":
    ch = ClickHouseConnector()

    if ch.health_check():
        print("ClickHouse connection successful!")

        try:
            df = ch.query_df("SELECT * FROM system.tables LIMIT 5")
            print(f"Found {len(df)} tables")
            version = ch.command("SELECT version()")
            print(f"ClickHouse version: {version}")
        except Exception as e:
            print(f"Operation failed: {e}")
    else:
        print("ClickHouse connection failed!")

    # Example with custom settings
    ch_ml = ClickHouseConnector(
        connect_timeout=30,
        send_receive_timeout=600,
        compress="zstd",
        settings={
            "max_memory_usage": "20000000000",
            "max_execution_time": "3600",
        },
    )
