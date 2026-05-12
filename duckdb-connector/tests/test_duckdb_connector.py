from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Tests in this file require the OpenMetadata SDK at runtime. Skip cleanly
# when running in an environment that doesn't have it installed.
pytest.importorskip("metadata.ingestion.api.steps")

from connector.duckdb_connector import (  # noqa: E402
    DuckDBConnector,
    InvalidDuckDBConnectorException,
)


def _make_config_mock(options: dict) -> MagicMock:
    """Build a WorkflowSource-shaped mock for unit-testing __init__."""
    cfg = MagicMock()
    cfg.serviceName = "duckdb_local"
    cfg.serviceConnection.root.config.connectionOptions.root = options
    return cfg


def _build_connector(
    db_path: Path | str,
    schemas: str = "sales,marketing",
) -> DuckDBConnector:
    """Build a connector by bypassing __init__ — useful for testing yield_* in isolation."""
    c = object.__new__(DuckDBConnector)
    c.config = MagicMock(serviceName="duckdb_local")
    c.metadata = MagicMock()
    c.service_connection = MagicMock()
    c.database_name = "test_db"
    c.database_schema_list = [s.strip() for s in schemas.split(",") if s.strip()]
    c.database_file_path = str(db_path)
    return c


class TestInit:
    def test_missing_database_name_raises(self):
        cfg = _make_config_mock(
            {"database_schema_list": "a", "database_file_path": "/x"}
        )
        with pytest.raises(InvalidDuckDBConnectorException, match="database_name"):
            DuckDBConnector(cfg, MagicMock())

    def test_missing_schema_list_raises(self):
        cfg = _make_config_mock(
            {"database_name": "db", "database_file_path": "/x"}
        )
        with pytest.raises(InvalidDuckDBConnectorException, match="database_schema_list"):
            DuckDBConnector(cfg, MagicMock())

    def test_missing_file_path_raises(self):
        cfg = _make_config_mock(
            {"database_name": "db", "database_schema_list": "a"}
        )
        with pytest.raises(InvalidDuckDBConnectorException, match="database_file_path"):
            DuckDBConnector(cfg, MagicMock())

    def test_schema_list_strips_whitespace_and_empties(self):
        cfg = _make_config_mock(
            {
                "database_name": "db",
                "database_schema_list": " sales ,  , marketing,",
                "database_file_path": "/x",
            }
        )
        c = DuckDBConnector(cfg, MagicMock())
        assert c.database_schema_list == ["sales", "marketing"]


class TestPrepare:
    def test_missing_file_raises(self, tmp_path: Path):
        c = _build_connector(tmp_path / "missing.duckdb")
        with pytest.raises(InvalidDuckDBConnectorException, match="does not exist"):
            c.prepare()

    def test_existing_file_ok(self, duckdb_file: Path):
        c = _build_connector(duckdb_file)
        c.prepare()


class TestTestConnection:
    def test_connects_and_queries(self, duckdb_file: Path):
        c = _build_connector(duckdb_file)
        c.test_connection()


class TestIter:
    def test_iter_yields_full_sequence(self, duckdb_file: Path):
        c = _build_connector(duckdb_file, schemas="sales,marketing")

        def get_by_name(entity, fqn):
            stub = MagicMock()
            stub.fullyQualifiedName = fqn
            return stub

        c.metadata.get_by_name.side_effect = get_by_name
        c.metadata.get_create_service_from_source.return_value = MagicMock()

        results = list(c._iter())
        rights = [r.right for r in results if r.right is not None]

        # 1 service + 1 database + 2 schemas + 2 tables (orders, campaigns)
        assert len(rights) == 6
        type_names = [type(r).__name__ for r in rights]
        assert type_names[0].startswith("CreateDatabaseService") or type_names[0] == "MagicMock"
        assert type_names[1] == "CreateDatabaseRequest"
        assert {type_names[2], type_names[3]} == {"CreateDatabaseSchemaRequest"}
        assert {type_names[4], type_names[5]} == {"CreateTableRequest"}

    def test_yield_data_propagates_descriptions(self, duckdb_file: Path):
        c = _build_connector(duckdb_file, schemas="sales")

        def get_by_name(entity, fqn):
            stub = MagicMock()
            stub.fullyQualifiedName = fqn
            return stub

        c.metadata.get_by_name.side_effect = get_by_name

        results = list(c.yield_data())
        rights = [r.right for r in results if r.right is not None]
        assert len(rights) == 1
        orders = rights[0]

        assert orders.description.root == "Customer orders fact table"
        cols_by_name = {col.name.root: col for col in orders.columns}
        assert cols_by_name["total"].description.root == "Order total in USD"
        # No comment was set on `id` — description should be unset
        assert cols_by_name["id"].description is None

    def test_yield_data_skips_schemas_with_no_tables(self, duckdb_file: Path):
        # Schema isn't in the fixture DB at all → duckdb_tables() returns no
        # rows, so we yield zero CreateTableRequest entries for it. The sink
        # is responsible for rejecting tables in nonexistent schemas, not us.
        c = _build_connector(duckdb_file, schemas="sales,does_not_exist")

        def get_by_name(entity, fqn):
            stub = MagicMock()
            stub.fullyQualifiedName = fqn
            return stub

        c.metadata.get_by_name.side_effect = get_by_name

        results = list(c.yield_data())
        rights = [r.right for r in results if r.right is not None]
        assert len(rights) == 1  # only sales.orders


class TestCreate:
    def test_create_from_workflow_dict(self, workflow_config: dict):
        connector = DuckDBConnector.create(workflow_config, MagicMock())
        assert connector.database_name == "test_db"
        assert connector.database_schema_list == ["sales", "marketing"]


class TestMotherDuck:
    def test_is_motherduck_detects_md_prefix(self):
        c = _build_connector("md:my_db")
        assert c.is_motherduck is True

    def test_is_motherduck_false_for_local_path(self, duckdb_file: Path):
        c = _build_connector(duckdb_file)
        assert c.is_motherduck is False

    def test_prepare_skips_file_check_for_motherduck(self):
        c = _build_connector("md:nonexistent_db")
        c.prepare()  # must not raise even though no local file exists

    def test_connect_motherduck_uri_passes_through(self, monkeypatch):
        captured = {}

        def fake_connect(database, read_only, **kwargs):
            captured["database"] = database
            captured["read_only"] = read_only
            captured["kwargs"] = kwargs
            return MagicMock()

        monkeypatch.setattr("connector.duckdb_connector.duckdb.connect", fake_connect)

        c = _build_connector("md:my_db")
        c._connect()

        # The token is read by DuckDB itself from the motherduck_token env var;
        # the connector just forwards the URI as-is.
        assert captured["database"] == "md:my_db"
        assert captured["read_only"] is True
        assert captured["kwargs"] == {}

    def test_connect_local_path_passes_through(self, duckdb_file: Path, monkeypatch):
        captured = {}

        def fake_connect(database, read_only, **kwargs):
            captured["database"] = database
            captured["kwargs"] = kwargs
            return MagicMock()

        monkeypatch.setattr("connector.duckdb_connector.duckdb.connect", fake_connect)

        c = _build_connector(duckdb_file)
        c._connect()

        assert captured["database"] == str(duckdb_file)
        assert captured["kwargs"] == {}
