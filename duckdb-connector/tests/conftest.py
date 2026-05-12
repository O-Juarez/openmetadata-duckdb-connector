from pathlib import Path

import duckdb
import pytest


@pytest.fixture
def duckdb_file(tmp_path: Path) -> Path:
    # File basename (without extension) becomes the attached DuckDB database name
    # and must match `database_name` in the connector config.
    db_path = tmp_path / "test_db.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE SCHEMA sales")
        conn.execute(
            "CREATE TABLE sales.orders ("
            "id INTEGER, total DECIMAL(10,2), placed_at TIMESTAMP"
            ")"
        )
        conn.execute("COMMENT ON TABLE sales.orders IS 'Customer orders fact table'")
        conn.execute("COMMENT ON COLUMN sales.orders.total IS 'Order total in USD'")
        conn.execute("CREATE SCHEMA marketing")
        conn.execute(
            "CREATE TABLE marketing.campaigns ("
            "name VARCHAR, sent_at TIMESTAMP WITH TIME ZONE"
            ")"
        )
    finally:
        conn.close()
    return db_path


@pytest.fixture
def workflow_config(duckdb_file: Path) -> dict:
    return {
        "type": "customDatabase",
        "serviceName": "duckdb_local",
        "serviceConnection": {
            "config": {
                "type": "CustomDatabase",
                "sourcePythonClass": "connector.duckdb_connector.DuckDBConnector",
                "connectionOptions": {
                    "database_name": "test_db",
                    "database_schema_list": "sales, marketing",
                    "database_file_path": str(duckdb_file),
                },
            }
        },
        "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
    }
