#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Custom Database Service extracting metadata from a DuckDB database.
"""
import traceback
from pathlib import Path
from typing import Iterable, Optional

import duckdb

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import Column
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException, Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils import fqn
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class InvalidDuckDBConnectorException(Exception):
    """Connector configuration is not valid."""


_TYPE_PREFIX_MAP = {
    "DECIMAL": "DECIMAL",
    "TIMESTAMP": "TIMESTAMP",
}
_TYPE_EXACT_MAP = {
    "INTEGER": "INT",
}

# OM rejects VARCHAR/CHAR/BINARY/VARBINARY columns without a dataLength.
# DuckDB doesn't enforce string lengths, so use a placeholder.
_TYPES_REQUIRING_LENGTH = {"VARCHAR", "CHAR", "BINARY", "VARBINARY"}
_DEFAULT_LENGTH = 1


def _normalize_type(duckdb_type: str) -> str:
    if duckdb_type in _TYPE_EXACT_MAP:
        return _TYPE_EXACT_MAP[duckdb_type]
    for prefix, mapped in _TYPE_PREFIX_MAP.items():
        if duckdb_type.startswith(prefix):
            return mapped
    return duckdb_type


class DuckDBConnector(Source):
    """
    Reads metadata from a local DuckDB file and yields OpenMetadata
    Create*Request entities for the configured schemas.
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__()
        self.config = config
        self.metadata = metadata
        self.service_connection = config.serviceConnection.root.config

        options = self.service_connection.connectionOptions.root

        self.database_name: str = options.get("database_name")
        if not self.database_name:
            raise InvalidDuckDBConnectorException(
                "Missing database_name connection option"
            )

        schemas_raw: Optional[str] = options.get("database_schema_list")
        if not schemas_raw:
            raise InvalidDuckDBConnectorException(
                "Missing database_schema_list connection option"
            )
        self.database_schema_list = [
            s for s in (entry.strip() for entry in schemas_raw.split(",")) if s
        ]

        self.database_file_path: str = options.get("database_file_path")
        if not self.database_file_path:
            raise InvalidDuckDBConnectorException(
                "Missing database_file_path connection option"
            )

    @classmethod
    def create(
        cls,
        config_dict: dict,
        metadata: OpenMetadata,
        pipeline_name: Optional[str] = None,
    ) -> "DuckDBConnector":
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: CustomDatabaseConnection = config.serviceConnection.root.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected CustomDatabaseConnection, but got {connection}"
            )
        return cls(config, metadata)

    @property
    def is_motherduck(self) -> bool:
        return self.database_file_path.startswith("md:")

    def _connect(self):
        """Open a read-only DuckDB connection (local file or MotherDuck).

        For MotherDuck URIs, the token is read from the `motherduck_token`
        environment variable, which DuckDB picks up natively.
        """
        return duckdb.connect(database=self.database_file_path, read_only=True)

    def prepare(self):
        if self.is_motherduck:
            return
        path = Path(self.database_file_path)
        if not path.exists():
            raise InvalidDuckDBConnectorException(
                f"Source database file does not exist: {self.database_file_path}"
            )

    def yield_create_request_database_service(self) -> Iterable[Either]:
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=DatabaseService, config=self.config
            )
        )

    def yield_create_request_database(self) -> Iterable[Either]:
        service: DatabaseService = self.metadata.get_by_name(
            entity=DatabaseService, fqn=self.config.serviceName
        )
        yield Either(
            right=CreateDatabaseRequest(
                name=self.database_name,
                service=service.fullyQualifiedName,
            )
        )

    def yield_create_request_schema(self) -> Iterable[Either]:
        database_fqn = fqn._build(self.config.serviceName, self.database_name)
        database: Database = self.metadata.get_by_name(entity=Database, fqn=database_fqn)
        for schema_name in self.database_schema_list:
            yield Either(
                right=CreateDatabaseSchemaRequest(
                    name=schema_name,
                    database=database.fullyQualifiedName,
                )
            )

    def yield_data(self) -> Iterable[Either]:
        with self._connect() as conn:
            for schema_name in self.database_schema_list:
                # Use the FQN we built rather than fetching the schema entity,
                # because the bulk sink may not have flushed the schema yet
                # by the time we get here.
                schema_fqn = fqn._build(
                    self.config.serviceName, self.database_name, schema_name
                )

                tables = conn.execute(
                    "SELECT table_name, comment FROM duckdb_tables() "
                    "WHERE database_name = ? AND schema_name = ?",
                    [self.database_name, schema_name],
                ).fetchall()

                for table_name, table_comment in tables:
                    try:
                        columns = conn.execute(
                            "SELECT column_name, data_type, comment FROM duckdb_columns() "
                            "WHERE database_name = ? AND schema_name = ? "
                            "AND table_name = ? "
                            "ORDER BY column_index",
                            [self.database_name, schema_name, table_name],
                        ).fetchall()
                        yield Either(
                            right=CreateTableRequest(
                                name=table_name,
                                description=table_comment or None,
                                databaseSchema=schema_fqn,
                                columns=[
                                    self._build_column(col_name, col_type, col_comment)
                                    for col_name, col_type, col_comment in columns
                                ],
                            )
                        )
                    except Exception:
                        yield Either(
                            left=StackTraceError(
                                name=f"{schema_fqn}.{table_name}",
                                error=f"Failed to ingest table {table_name}",
                                stackTrace=traceback.format_exc(),
                            )
                        )

    @staticmethod
    def _build_column(
        col_name: str, col_type: str, col_comment: Optional[str] = None
    ) -> Column:
        data_type = _normalize_type(col_type)
        kwargs = {"name": col_name, "dataType": data_type}
        if data_type in _TYPES_REQUIRING_LENGTH:
            kwargs["dataLength"] = _DEFAULT_LENGTH
        if col_comment:
            kwargs["description"] = col_comment
        return Column(**kwargs)

    def _iter(self) -> Iterable[Entity]:
        yield from self.yield_create_request_database_service()
        yield from self.yield_create_request_database()
        yield from self.yield_create_request_schema()
        yield from self.yield_data()

    def test_connection(self) -> None:
        with self._connect() as conn:
            conn.execute("SELECT 1").fetchone()

    def close(self):
        pass
