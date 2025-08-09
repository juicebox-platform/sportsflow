import pytest
import os
import pandas as pd
from unittest.mock import Mock, patch
from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError

from sportsflow.core.clickhouse import ClickHouseConnector


class TestClickHouseConnector:
    """Test suite for ClickHouseConnector class."""

    def setup_method(self):
        """Setup method run before each test."""
        # Clear environment variables that might affect tests
        self.original_env = {}
        env_vars = [
            "CLICKHOUSE_USERNAME",
            "CLICKHOUSE_PASSWORD",
            "CLICKHOUSE_DATABASE",
            "KUBERNETES_SERVICE_HOST",
            "KUBERNETES_PORT",
            "K3S_NODE_NAME",
        ]
        for var in env_vars:
            self.original_env[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]

    def teardown_method(self):
        """Cleanup method run after each test."""
        # Restore original environment variables
        for var, value in self.original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]

    @patch("os.path.exists")
    def test_detect_k3s_environment_service_account_token(self, mock_exists):
        """Test K3s detection via service account token."""
        mock_exists.return_value = True

        with patch("clickhouse_connect.get_client") as mock_get_client:
            mock_get_client.return_value = Mock()
            connector = ClickHouseConnector()
            assert connector.is_in_k3s is True

    def test_detect_k3s_environment_kubernetes_service_host(self):
        """Test K3s detection via KUBERNETES_SERVICE_HOST."""
        os.environ["KUBERNETES_SERVICE_HOST"] = "10.43.0.1"

        with patch("clickhouse_connect.get_client") as mock_get_client:
            mock_get_client.return_value = Mock()
            connector = ClickHouseConnector()
            assert connector.is_in_k3s is True

    def test_detect_k3s_environment_k3s_node_name(self):
        """Test K3s detection via K3S_NODE_NAME."""
        os.environ["K3S_NODE_NAME"] = "worker-node-1"

        with patch("clickhouse_connect.get_client") as mock_get_client:
            mock_get_client.return_value = Mock()
            connector = ClickHouseConnector()
            assert connector.is_in_k3s is True

    @patch("os.path.exists")
    def test_detect_local_environment(self, mock_exists):
        """Test local environment detection (not in K3s)."""
        mock_exists.return_value = False

        with patch("clickhouse_connect.get_client") as mock_get_client:
            mock_get_client.return_value = Mock()
            connector = ClickHouseConnector()
            assert connector.is_in_k3s is False

    @patch("os.path.exists")
    @patch("clickhouse_connect.get_client")
    def test_local_connection_config(self, mock_get_client, mock_exists):
        """Test that local environment uses external connection settings."""
        mock_exists.return_value = False
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()

        # Verify get_client was called with external settings
        mock_get_client.assert_called_once()
        call_args = mock_get_client.call_args[1]
        assert call_args["host"] == "clickhouse-api.juicebox.com"
        assert call_args["port"] == 443
        assert call_args["secure"] is True
        assert call_args["verify"] is False

    @patch("os.path.exists")
    @patch("clickhouse_connect.get_client")
    def test_k3s_connection_config(self, mock_get_client, mock_exists):
        """Test that K3s environment uses internal connection settings."""
        mock_exists.return_value = True
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()

        # Verify get_client was called with internal settings
        mock_get_client.assert_called_once()
        call_args = mock_get_client.call_args[1]
        assert call_args["host"] == "clickhouse-clickhouse-cluster"
        assert call_args["port"] == 8123
        assert call_args["secure"] is False
        assert call_args["verify"] is True

    def test_environment_variable_credentials(self):
        """Test that environment variables override default credentials."""
        os.environ["CLICKHOUSE_USERNAME"] = "test_user"
        os.environ["CLICKHOUSE_PASSWORD"] = "test_pass"
        os.environ["CLICKHOUSE_DATABASE"] = "test_db"

        with patch("clickhouse_connect.get_client") as mock_get_client:
            mock_get_client.return_value = Mock()

            connector = ClickHouseConnector()

            assert connector.username == "test_user"
            assert connector.password == "test_pass"
            assert connector.database == "test_db"

    def test_parameter_credentials_override_env(self):
        """Test that constructor parameters override environment variables."""
        os.environ["CLICKHOUSE_USERNAME"] = "env_user"
        os.environ["CLICKHOUSE_PASSWORD"] = "env_pass"

        with patch("clickhouse_connect.get_client") as mock_get_client:
            mock_get_client.return_value = Mock()

            connector = ClickHouseConnector(
                username="param_user", password="param_pass"
            )

            assert connector.username == "param_user"
            assert connector.password == "param_pass"

    @patch("clickhouse_connect.get_client")
    def test_custom_kwargs_passed_through(self, mock_get_client):
        """Test that custom kwargs are passed to clickhouse_connect.get_client."""
        mock_get_client.return_value = Mock()

        custom_kwargs = {
            "connect_timeout": 30,
            "send_receive_timeout": 600,
            "compress": "zstd",
        }

        ClickHouseConnector(**custom_kwargs)

        call_args = mock_get_client.call_args[1]
        for key, value in custom_kwargs.items():
            assert call_args[key] == value

    @patch("clickhouse_connect.get_client")
    def test_health_check_success(self, mock_get_client):
        """Test successful health check."""
        mock_client = Mock()
        mock_client.command.return_value = 1
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        assert connector.health_check() is True
        mock_client.command.assert_called_with("SELECT 1")

    @patch("clickhouse_connect.get_client")
    def test_health_check_failure(self, mock_get_client):
        """Test health check failure."""
        mock_client = Mock()
        mock_client.command.side_effect = OperationalError("Connection failed")
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        assert connector.health_check() is False

    @patch("clickhouse_connect.get_client")
    def test_execute_query(self, mock_get_client):
        """Test execute_query method."""
        mock_client = Mock()
        mock_result = Mock()
        mock_client.query.return_value = mock_result
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        result = connector.execute_query(
            "SELECT * FROM test_table",
            parameters={"param1": "value1"},
            settings={"max_memory_usage": "1000000"},
        )

        assert result == mock_result
        mock_client.query.assert_called_once_with(
            "SELECT * FROM test_table",
            parameters={"param1": "value1"},
            settings={"max_memory_usage": "1000000"},
        )

    @patch("clickhouse_connect.get_client")
    def test_query_df(self, mock_get_client):
        """Test query_df method."""
        mock_client = Mock()
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_client.query_df.return_value = mock_df
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        result = connector.query_df("SELECT * FROM test_table")

        pd.testing.assert_frame_equal(result, mock_df)
        mock_client.query_df.assert_called_once_with(
            "SELECT * FROM test_table", parameters=None, settings=None
        )

    @patch("clickhouse_connect.get_client")
    def test_insert_df(self, mock_get_client):
        """Test insert_df method."""
        mock_client = Mock()
        mock_summary = {"written_rows": 3, "written_bytes": 150}
        mock_client.insert_df.return_value = mock_summary
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        result = connector.insert_df("test_table", test_df)

        assert result == mock_summary
        mock_client.insert_df.assert_called_once_with(
            "test_table", test_df, database=None, settings=None
        )

    @patch("clickhouse_connect.get_client")
    def test_insert_data(self, mock_get_client):
        """Test insert_data method."""
        mock_client = Mock()
        mock_summary = {"written_rows": 2, "written_bytes": 100}
        mock_client.insert.return_value = mock_summary
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        test_data = [[1, "a"], [2, "b"]]

        result = connector.insert_data(
            "test_table", test_data, column_names=["id", "name"]
        )

        assert result == mock_summary
        mock_client.insert.assert_called_once_with(
            "test_table",
            test_data,
            column_names=["id", "name"],
            database=None,
            settings=None,
        )

    @patch("clickhouse_connect.get_client")
    def test_command(self, mock_get_client):
        """Test command method."""
        mock_client = Mock()
        mock_client.command.return_value = "24.8.1"
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        result = connector.command("SELECT version()")

        assert result == "24.8.1"
        mock_client.command.assert_called_once_with(
            "SELECT version()", parameters=None, settings=None
        )

    @patch("clickhouse_connect.get_client")
    def test_get_table_schema(self, mock_get_client):
        """Test get_table_schema method."""
        mock_client = Mock()
        mock_schema_df = pd.DataFrame(
            {
                "name": ["id", "name", "created_at"],
                "type": ["UInt64", "String", "DateTime"],
                "default_type": ["", "", ""],
                "default_expression": ["", "", ""],
            }
        )
        mock_client.query_df.return_value = mock_schema_df
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        result = connector.get_table_schema("test_table", "test_db")

        pd.testing.assert_frame_equal(result, mock_schema_df)
        mock_client.query_df.assert_called_once_with(
            "DESCRIBE TABLE test_db.test_table"
        )

    @patch("clickhouse_connect.get_client")
    def test_close_method(self, mock_get_client):
        """Test close method."""
        mock_client = Mock()
        mock_client.close = Mock()
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        connector.close()

        mock_client.close.assert_called_once()

    @patch("clickhouse_connect.get_client")
    def test_close_method_no_close_attribute(self, mock_get_client):
        """Test close method when client has no close attribute."""
        mock_client = Mock()
        del mock_client.close  # Remove close attribute
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        # Should not raise an exception
        connector.close()

    def test_custom_external_settings(self):
        """Test custom external connection settings."""
        with patch("clickhouse_connect.get_client") as mock_get_client:
            mock_get_client.return_value = Mock()

            connector = ClickHouseConnector(
                external_host="custom-host.com",
                external_port=9999,
                external_secure=False,
                external_verify_ssl=True,
            )

            call_args = mock_get_client.call_args[1]
            assert call_args["host"] == "custom-host.com"
            assert call_args["port"] == 9999
            assert call_args["secure"] is False
            assert call_args["verify"] is True

    @patch("os.path.exists")
    def test_custom_internal_settings(self, mock_exists):
        """Test custom internal connection settings."""
        mock_exists.return_value = True  # Simulate K3s environment

        with patch("clickhouse_connect.get_client") as mock_get_client:
            mock_get_client.return_value = Mock()

            connector = ClickHouseConnector(
                internal_host="custom-internal-host",
                internal_port=9999,
                internal_secure=True,
                internal_verify_ssl=False,
            )

            call_args = mock_get_client.call_args[1]
            assert call_args["host"] == "custom-internal-host"
            assert call_args["port"] == 9999
            assert call_args["secure"] is True
            assert call_args["verify"] is False

    @patch("clickhouse_connect.get_client")
    def test_database_not_added_when_none(self, mock_get_client):
        """Test that database is not added to config when None."""
        mock_get_client.return_value = Mock()

        ClickHouseConnector(database=None)

        call_args = mock_get_client.call_args[1]
        assert "database" not in call_args

    @patch("clickhouse_connect.get_client")
    def test_database_added_when_specified(self, mock_get_client):
        """Test that database is added to config when specified."""
        mock_get_client.return_value = Mock()

        ClickHouseConnector(database="test_db")

        call_args = mock_get_client.call_args[1]
        assert call_args["database"] == "test_db"


class TestClickHouseConnectorIntegration:
    """Integration tests that test method interactions."""

    @patch("clickhouse_connect.get_client")
    def test_etl_workflow(self, mock_get_client):
        """Test a typical ETL workflow."""
        mock_client = Mock()

        # Mock query_df to return sample data
        sample_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, 20.3, 30.1],
            }
        )
        mock_client.query_df.return_value = sample_data

        # Mock insert_df to return summary
        insert_summary = {"written_rows": 3, "written_bytes": 150}
        mock_client.insert_df.return_value = insert_summary

        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()

        # Extract data
        df = connector.query_df("SELECT * FROM source_table")
        assert len(df) == 3

        # Transform data (simple example)
        df["transformed_value"] = df["value"] * 2

        # Load data
        result = connector.insert_df("target_table", df)
        assert result["written_rows"] == 3

    @patch("clickhouse_connect.get_client")
    def test_ml_workflow_with_parameters(self, mock_get_client):
        """Test ML workflow with parameterized queries."""
        mock_client = Mock()

        # Mock training data query
        training_data = pd.DataFrame(
            {
                "feature1": [1.0, 2.0, 3.0],
                "feature2": [0.5, 1.5, 2.5],
                "target": [0, 1, 1],
            }
        )
        mock_client.query_df.return_value = training_data

        # Mock prediction insert
        insert_summary = {"written_rows": 100, "written_bytes": 1000}
        mock_client.insert_df.return_value = insert_summary

        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()

        # Get training data with parameters
        train_df = connector.query_df(
            "SELECT * FROM ml_features WHERE date >= {start_date:Date}",
            parameters={"start_date": "2025-01-01"},
        )

        assert len(train_df) == 3

        # Simulate ML predictions (dummy data)
        predictions_df = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "prediction": [0.8, 0.3, 0.9],
                "model_version": ["v1.0", "v1.0", "v1.0"],
            }
        )

        # Insert predictions
        result = connector.insert_df("ml_predictions", predictions_df)
        assert result["written_rows"] == 100


class TestClickHouseConnectorErrorHandling:
    """Test error handling scenarios."""

    @patch("clickhouse_connect.get_client")
    def test_query_error_propagation(self, mock_get_client):
        """Test that query errors are properly propagated."""
        mock_client = Mock()
        mock_client.query_df.side_effect = ProgrammingError("Syntax error in query")
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()

        with pytest.raises(ProgrammingError, match="Syntax error in query"):
            connector.query_df("INVALID SQL QUERY")

    @patch("clickhouse_connect.get_client")
    def test_insert_error_propagation(self, mock_get_client):
        """Test that insert errors are properly propagated."""
        mock_client = Mock()
        mock_client.insert_df.side_effect = OperationalError("Table does not exist")
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        test_df = pd.DataFrame({"col1": [1, 2, 3]})

        with pytest.raises(OperationalError, match="Table does not exist"):
            connector.insert_df("nonexistent_table", test_df)

    @patch("clickhouse_connect.get_client")
    def test_connection_creation_failure(self, mock_get_client):
        """Test handling of connection creation failures."""
        mock_get_client.side_effect = OperationalError("Cannot connect to server")

        with pytest.raises(OperationalError, match="Cannot connect to server"):
            ClickHouseConnector()


class TestClickHouseConnectorEdgeCases:
    """Test edge cases and corner scenarios."""

    @patch("os.path.exists")
    @patch("clickhouse_connect.get_client")
    def test_multiple_k3s_indicators(self, mock_get_client, mock_exists):
        """Test when multiple K3s indicators are present."""
        mock_exists.return_value = True
        os.environ["KUBERNETES_SERVICE_HOST"] = "10.43.0.1"
        os.environ["K3S_NODE_NAME"] = "node-1"

        mock_get_client.return_value = Mock()

        connector = ClickHouseConnector()
        assert connector.is_in_k3s is True

    @patch("clickhouse_connect.get_client")
    def test_empty_credentials(self, mock_get_client):
        """Test behavior with empty credentials."""
        mock_get_client.return_value = Mock()

        connector = ClickHouseConnector(username="", password="")

        assert connector.username == ""
        assert connector.password == ""

    @patch("clickhouse_connect.get_client")
    def test_get_table_schema_with_database(self, mock_get_client):
        """Test get_table_schema with explicit database."""
        mock_client = Mock()
        mock_schema = pd.DataFrame({"name": ["col1"], "type": ["String"]})
        mock_client.query_df.return_value = mock_schema
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        connector.get_table_schema("test_table", "test_db")

        mock_client.query_df.assert_called_once_with(
            "DESCRIBE TABLE test_db.test_table"
        )

    @patch("clickhouse_connect.get_client")
    def test_get_table_schema_without_database(self, mock_get_client):
        """Test get_table_schema without explicit database."""
        mock_client = Mock()
        mock_schema = pd.DataFrame({"name": ["col1"], "type": ["String"]})
        mock_client.query_df.return_value = mock_schema
        mock_get_client.return_value = mock_client

        connector = ClickHouseConnector()
        connector.get_table_schema("test_table")

        mock_client.query_df.assert_called_once_with("DESCRIBE TABLE test_table")


# Fixtures for integration testing
@pytest.fixture
def mock_clickhouse_client():
    """Fixture providing a mocked ClickHouse client."""
    with patch("clickhouse_connect.get_client") as mock_get_client:
        mock_client = Mock()
        mock_client.command.return_value = 1  # Health check
        mock_client.host = "test-host"
        mock_client.port = 8123
        mock_client.database = "default"
        mock_client.secure = False
        mock_get_client.return_value = mock_client
        yield mock_client


@pytest.fixture
def sample_dataframe():
    """Fixture providing sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "score": [95.5, 87.2, 92.1, 78.9, 88.3],
            "created_at": pd.to_datetime(
                ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"]
            ),
        }
    )


# Example integration test using fixtures
def test_complete_workflow(mock_clickhouse_client, sample_dataframe):
    """Test complete ETL workflow using fixtures."""
    # Setup return values
    mock_clickhouse_client.query_df.return_value = sample_dataframe
    mock_clickhouse_client.insert_df.return_value = {
        "written_rows": 5,
        "written_bytes": 250,
    }

    connector = ClickHouseConnector()

    # Test query
    df = connector.query_df("SELECT * FROM source_table")
    assert len(df) == 5
    assert "score" in df.columns

    # Test insert
    result = connector.insert_df("target_table", df)
    assert result["written_rows"] == 5


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
