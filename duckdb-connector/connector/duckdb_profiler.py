"""
Custom profiler for DuckDB / MotherDuck → OpenMetadata.

OpenMetadata 1.12.x ships no DuckDB connector, so the standard
`metadata profile -c …` workflow can't profile a customDatabase service.
This module fills the gap: it reads tables already registered in OM under
a given service, runs profiling queries directly against DuckDB, and PUTs
the results back into OM via the SDK.

Run:
    duckdb-profile -c duckdb_profiler.yml
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

import duckdb
import yaml

from metadata.generated.schema.api.data.createTableProfile import (
    CreateTableProfileRequest,
)
from metadata.generated.schema.entity.data.table import (
    ColumnProfile,
    Table,
    TableProfile,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata

logger = logging.getLogger("duckdb_profiler")


_NUMERIC_PREFIXES = (
    "INT",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "HUGEINT",
    "DOUBLE",
    "REAL",
    "FLOAT",
    "DECIMAL",
    "NUMERIC",
)
_TEMPORAL_PREFIXES = ("DATE", "TIMESTAMP", "TIME")
_STRING_PREFIXES = ("VARCHAR", "CHAR", "TEXT", "STRING")


def _is_numeric(t: str) -> bool:
    return t.upper().startswith(_NUMERIC_PREFIXES)


def _is_temporal(t: str) -> bool:
    return t.upper().startswith(_TEMPORAL_PREFIXES)


def _is_string(t: str) -> bool:
    return t.upper().startswith(_STRING_PREFIXES)


@dataclass
class ProfilerConfig:
    database_name: str
    database_url: str
    om_host_port: str
    om_jwt: str
    service_name: str
    schema_includes: List[str]
    table_includes: Optional[List[str]] = None
    profile_sample_pct: float = 100.0
    schema_excludes: Optional[List[str]] = None
    table_excludes: Optional[List[str]] = None

    @classmethod
    def from_yaml(cls, path: str) -> "ProfilerConfig":
        with open(path) as f:
            raw = yaml.safe_load(os.path.expandvars(f.read()))

        src = raw["source"]
        cfg = src["serviceConnection"]["config"]
        sample_cfg = src["sourceConfig"]["config"]
        wf_cfg = raw["workflowConfig"]["openMetadataServerConfig"]
        schema_filter = sample_cfg.get("schemaFilterPattern") or {}
        table_filter = sample_cfg.get("tableFilterPattern") or {}

        return cls(
            database_name=cfg["databaseName"],
            database_url=cfg["databaseUrl"],
            om_host_port=wf_cfg["hostPort"],
            om_jwt=wf_cfg["securityConfig"]["jwtToken"],
            service_name=src["serviceName"],
            schema_includes=schema_filter.get("includes", []),
            schema_excludes=schema_filter.get("excludes"),
            table_includes=table_filter.get("includes"),
            table_excludes=table_filter.get("excludes"),
            profile_sample_pct=float(sample_cfg.get("profileSample", 100)),
        )


def _quote_ident(name: str) -> str:
    """DuckDB identifier quoting — double-quote, escape internal quotes."""
    return '"' + name.replace('"', '""') + '"'


def _matches_any(value: str, patterns: Optional[Sequence[str]]) -> bool:
    if not patterns:
        return False
    return any(re.fullmatch(p, value) for p in patterns)


def _build_duckdb_conn(config: ProfilerConfig) -> duckdb.DuckDBPyConnection:
    # For MotherDuck URIs, DuckDB reads the token from the motherduck_token env var.
    return duckdb.connect(database=config.database_url, read_only=True)


def _build_om_client(config: ProfilerConfig) -> OpenMetadata:
    return OpenMetadata(
        OpenMetadataConnection(
            hostPort=config.om_host_port,
            authProvider="openmetadata",
            securityConfig=OpenMetadataJWTClientConfig(jwtToken=config.om_jwt),
        )
    )


def _list_tables(metadata: OpenMetadata, config: ProfilerConfig) -> Iterable[Table]:
    """Pull every Table under the service, applying the include/exclude filters."""
    after = None
    while True:
        page = metadata.list_entities(
            entity=Table,
            params={"service": config.service_name, "include": "non-deleted"},
            after=after,
            fields=["columns"],
            limit=200,
        )
        for table in page.entities:
            schema_name = table.databaseSchema.name
            if config.schema_includes and schema_name not in config.schema_includes:
                continue
            if _matches_any(schema_name, config.schema_excludes):
                continue
            table_name = table.name.root
            if config.table_includes and not _matches_any(table_name, config.table_includes):
                continue
            if _matches_any(table_name, config.table_excludes):
                continue
            yield table
        if not page.after:
            break
        after = page.after


def _column_metric_aliases(idx: int) -> dict:
    """Stable aliases for the projected column-level aggregates."""
    return {
        "values_count": f"vc_{idx}",
        "null_count": f"nc_{idx}",
        "distinct_count": f"dc_{idx}",
        "min": f"mn_{idx}",
        "max": f"mx_{idx}",
        "mean": f"me_{idx}",
        "stddev": f"sd_{idx}",
        "sum": f"sm_{idx}",
        "min_length": f"ml_{idx}",
        "max_length": f"xl_{idx}",
        "mean_length": f"al_{idx}",
    }


def _build_column_aggregates(idx: int, col_name: str, col_type: str) -> List[str]:
    safe = _quote_ident(col_name)
    a = _column_metric_aliases(idx)
    parts = [
        f"COUNT({safe}) AS {a['values_count']}",
        f"COUNT(*) - COUNT({safe}) AS {a['null_count']}",
        f"COUNT(DISTINCT {safe}) AS {a['distinct_count']}",
    ]
    if _is_numeric(col_type):
        parts += [
            f"MIN({safe})::VARCHAR AS {a['min']}",
            f"MAX({safe})::VARCHAR AS {a['max']}",
            f"AVG({safe}::DOUBLE) AS {a['mean']}",
            f"STDDEV({safe}::DOUBLE) AS {a['stddev']}",
            f"SUM({safe}::DOUBLE) AS {a['sum']}",
        ]
    elif _is_temporal(col_type):
        parts += [
            f"MIN({safe})::VARCHAR AS {a['min']}",
            f"MAX({safe})::VARCHAR AS {a['max']}",
        ]
    elif _is_string(col_type):
        parts += [
            f"MIN(LENGTH({safe})) AS {a['min_length']}",
            f"MAX(LENGTH({safe})) AS {a['max_length']}",
            f"AVG(LENGTH({safe})::DOUBLE) AS {a['mean_length']}",
        ]
    return parts


def _profile_table(
    conn: duckdb.DuckDBPyConnection,
    config: ProfilerConfig,
    table: Table,
) -> Optional[CreateTableProfileRequest]:
    """Run profiling SQL for one table and build the OM profile request."""
    schema_name = table.databaseSchema.name
    table_name = table.name.root
    qualified = f'{_quote_ident(config.database_name)}.{_quote_ident(schema_name)}.{_quote_ident(table_name)}'

    # Skip tables with no columns to profile
    columns = list(table.columns or [])
    if not columns:
        return None

    select_parts = ["COUNT(*) AS row_count"]
    for idx, col in enumerate(columns):
        select_parts.extend(_build_column_aggregates(idx, col.name.root, col.dataType.value))

    sample_clause = ""
    if 0 < config.profile_sample_pct < 100:
        sample_clause = f" USING SAMPLE {config.profile_sample_pct} PERCENT"

    sql = f"SELECT {', '.join(select_parts)} FROM {qualified}{sample_clause}"
    logger.debug("Profiling %s.%s with %d aggregates", schema_name, table_name, len(select_parts))

    row = conn.execute(sql).fetchone()
    if row is None:
        return None
    row_count = int(row[0] or 0)
    cursor = 1

    column_profiles: List[ColumnProfile] = []
    timestamp_ms = int(time.time() * 1000)

    for idx, col in enumerate(columns):
        col_type = col.dataType.value
        values_count = row[cursor]; cursor += 1
        null_count = row[cursor]; cursor += 1
        distinct_count = row[cursor]; cursor += 1

        kwargs: dict = {
            "name": col.name.root,
            "timestamp": timestamp_ms,
            "valuesCount": values_count,
            "nullCount": null_count,
            "distinctCount": distinct_count,
        }
        if values_count and (null_count is not None):
            total = values_count + null_count
            if total:
                kwargs["nullProportion"] = null_count / total
        if values_count and distinct_count is not None and values_count > 0:
            kwargs["distinctProportion"] = distinct_count / values_count

        if _is_numeric(col_type):
            mn, mx, mean, stddev, sm = (row[cursor + i] for i in range(5))
            cursor += 5
            kwargs["min"] = mn
            kwargs["max"] = mx
            kwargs["mean"] = mean
            kwargs["stddev"] = stddev
            kwargs["sum"] = sm
        elif _is_temporal(col_type):
            mn, mx = row[cursor], row[cursor + 1]
            cursor += 2
            kwargs["min"] = mn
            kwargs["max"] = mx
        elif _is_string(col_type):
            min_len, max_len, mean_len = (row[cursor + i] for i in range(3))
            cursor += 3
            kwargs["minLength"] = min_len
            kwargs["maxLength"] = max_len
            kwargs["mean"] = mean_len  # OM stores mean length under `mean` for strings

        column_profiles.append(ColumnProfile(**kwargs))

    table_profile = TableProfile(
        timestamp=timestamp_ms,
        rowCount=row_count,
        columnCount=len(columns),
        profileSample=config.profile_sample_pct if config.profile_sample_pct < 100 else None,
        profileSampleType="PERCENTAGE" if config.profile_sample_pct < 100 else None,
    )
    return CreateTableProfileRequest(
        tableProfile=table_profile,
        columnProfile=column_profiles,
    )


def run_profiler(config: ProfilerConfig) -> None:
    metadata = _build_om_client(config)
    conn = _build_duckdb_conn(config)
    tables = list(_list_tables(metadata, config))
    logger.info("Profiling %d tables under service %s", len(tables), config.service_name)

    succeeded = failed = 0
    try:
        for table in tables:
            schema_name = table.databaseSchema.name
            table_name = table.name.root
            try:
                request = _profile_table(conn, config, table)
                if request is None:
                    logger.warning("Skipping %s.%s — no columns", schema_name, table_name)
                    continue
                metadata.ingest_profile_data(table=table, profile_request=request)
                succeeded += 1
                logger.info("Profiled %s.%s (%d rows, %d cols)",
                            schema_name, table_name,
                            request.tableProfile.rowCount, request.tableProfile.columnCount)
            except Exception as exc:
                failed += 1
                logger.error("Failed profiling %s.%s: %s", schema_name, table_name, exc)
    finally:
        conn.close()
    logger.info("Profiling done — %d succeeded, %d failed", succeeded, failed)
    if failed and not succeeded:
        sys.exit(1)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("-c", "--config", required=True, help="Profiler YAML config")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )
    config = ProfilerConfig.from_yaml(args.config)
    run_profiler(config)


if __name__ == "__main__":
    main()
