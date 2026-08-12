from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from supply_chain_ops.config import ProjectPaths

EXPECTED_COLUMNS = {
    "shipments": [
        "shipment_id",
        "created_ts",
        "promised_delivery_ts",
        "origin_hub_id",
        "destination_hub_id",
        "route_id",
        "service_level_id",
        "customer_region",
        "weight_kg",
    ],
    "events": ["event_id", "shipment_id", "event_sequence", "event_type", "event_ts", "hub_id"],
    "hubs": ["hub_id", "hub_name", "city", "state_code"],
    "routes": ["route_id", "origin_hub_id", "destination_hub_id", "distance_km"],
    "service_levels": ["service_level_id", "service_level_name", "sla_days", "base_price_eur"],
}

TABLE_FILE_KINDS = {
    "source/shipments/shipments.parquet": "shipments",
    "source/reference/hubs.parquet": "hubs",
    "source/reference/routes.parquet": "routes",
    "source/reference/service_levels.parquet": "service_levels",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_kind(relative_path: str) -> str:
    if relative_path in TABLE_FILE_KINDS:
        return TABLE_FILE_KINDS[relative_path]
    if relative_path.startswith("source/shipment_events/batch_date="):
        return "events"
    raise ValueError(f"unexpected source file: {relative_path}")


def _validate_parquet_schema(path: Path, kind: str) -> None:
    schema = pq.read_schema(path)
    names = schema.names
    if names != EXPECTED_COLUMNS[kind]:
        raise ValueError(
            f"schema mismatch for {path}: expected {EXPECTED_COLUMNS[kind]}, received {names}"
        )
    type_expectations: dict[str, tuple[str, ...]] = {
        "shipment_id": ("string",),
        "event_id": ("string",),
        "event_sequence": ("int8", "int16", "int32", "int64"),
        "created_ts": ("timestamp",),
        "promised_delivery_ts": ("timestamp",),
        "event_ts": ("timestamp",),
        "weight_kg": ("double", "float"),
        "distance_km": ("int",),
    }
    for field in schema:
        expected_prefixes = type_expectations.get(field.name)
        if expected_prefixes and not str(field.type).startswith(expected_prefixes):
            raise ValueError(f"type mismatch for {path}:{field.name}: received {field.type}")


def _sql_path(name: str) -> Path:
    return Path(__file__).resolve().parents[2] / "sql" / name


def _execute_sql_file(connection: duckdb.DuckDBPyConnection, name: str) -> None:
    connection.execute(_sql_path(name).read_text(encoding="utf-8"))


def _create_control_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_pipeline_runs (
            run_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            status VARCHAR NOT NULL,
            discovered_files BIGINT DEFAULT 0,
            loaded_files BIGINT DEFAULT 0,
            skipped_files BIGINT DEFAULT 0,
            error_message VARCHAR
        );
        CREATE TABLE IF NOT EXISTS meta_ingestion_files (
            source_file VARCHAR PRIMARY KEY,
            file_kind VARCHAR NOT NULL,
            sha256 VARCHAR NOT NULL,
            file_size BIGINT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL,
            run_id VARCHAR NOT NULL,
            status VARCHAR NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bronze_shipments (
            shipment_id VARCHAR,
            created_ts TIMESTAMP,
            promised_delivery_ts TIMESTAMP,
            origin_hub_id VARCHAR,
            destination_hub_id VARCHAR,
            route_id VARCHAR,
            service_level_id VARCHAR,
            customer_region VARCHAR,
            weight_kg DOUBLE,
            source_file VARCHAR NOT NULL,
            source_sha256 VARCHAR NOT NULL,
            source_row BIGINT NOT NULL,
            ingestion_run_id VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bronze_events (
            event_id VARCHAR,
            shipment_id VARCHAR,
            event_sequence SMALLINT,
            event_type VARCHAR,
            event_ts TIMESTAMP,
            hub_id VARCHAR,
            source_file VARCHAR NOT NULL,
            source_sha256 VARCHAR NOT NULL,
            source_row BIGINT NOT NULL,
            ingestion_run_id VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bronze_hubs (
            hub_id VARCHAR, hub_name VARCHAR, city VARCHAR, state_code VARCHAR,
            source_file VARCHAR NOT NULL, source_sha256 VARCHAR NOT NULL,
            source_row BIGINT NOT NULL, ingestion_run_id VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bronze_routes (
            route_id VARCHAR, origin_hub_id VARCHAR,
            destination_hub_id VARCHAR, distance_km INTEGER,
            source_file VARCHAR NOT NULL, source_sha256 VARCHAR NOT NULL,
            source_row BIGINT NOT NULL, ingestion_run_id VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bronze_service_levels (
            service_level_id VARCHAR, service_level_name VARCHAR, sla_days INTEGER,
            base_price_eur DOUBLE, source_file VARCHAR NOT NULL, source_sha256 VARCHAR NOT NULL,
            source_row BIGINT NOT NULL, ingestion_run_id VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL
        );
        """
    )


def _target_table(kind: str) -> str:
    return {
        "shipments": "bronze_shipments",
        "events": "bronze_events",
        "hubs": "bronze_hubs",
        "routes": "bronze_routes",
        "service_levels": "bronze_service_levels",
    }[kind]


def _load_file(
    connection: duckdb.DuckDBPyConnection,
    absolute_path: Path,
    relative_path: str,
    kind: str,
    file_hash: str,
    run_id: str,
) -> None:
    table = _target_table(kind)
    columns = ", ".join(EXPECTED_COLUMNS[kind])
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"DELETE FROM {table} WHERE source_file = ?", [relative_path])
        connection.execute(
            f"""
            INSERT INTO {table}
            SELECT {columns}, ?, ?, row_number() OVER (), ?, current_timestamp
            FROM read_parquet(?)
            """,
            [relative_path, file_hash, run_id, str(absolute_path)],
        )
        connection.execute(
            "DELETE FROM meta_ingestion_files WHERE source_file = ?", [relative_path]
        )
        connection.execute(
            """
            INSERT INTO meta_ingestion_files
            VALUES (?, ?, ?, ?, current_timestamp, ?, 'SUCCESS')
            """,
            [relative_path, kind, file_hash, absolute_path.stat().st_size, run_id],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _export_tables(connection: duckdb.DuckDBPyConnection, paths: ProjectPaths) -> None:
    paths.silver.mkdir(parents=True, exist_ok=True)
    paths.gold.mkdir(parents=True, exist_ok=True)
    silver_tables = ["silver_shipments", "silver_events", "quarantine_records"]
    gold_tables = [
        "dim_date",
        "dim_hub",
        "dim_route",
        "dim_service_level",
        "fact_shipments",
        "fact_events",
        "fact_data_quality",
    ]
    for table in silver_tables:
        destination = paths.silver / f"{table}.parquet"
        connection.execute(
            f"COPY {table} TO ? (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)",
            [str(destination)],
        )
    for table in gold_tables:
        destination = paths.gold / f"{table}.parquet"
        connection.execute(
            f"COPY {table} TO ? (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)",
            [str(destination)],
        )


def _table_counts(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    tables = [
        "bronze_shipments",
        "bronze_events",
        "silver_shipments",
        "silver_events",
        "quarantine_records",
        "dim_date",
        "dim_hub",
        "dim_route",
        "dim_service_level",
        "fact_shipments",
        "fact_events",
        "fact_data_quality",
    ]
    return {
        table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in tables
    }


def run_pipeline(paths: ProjectPaths) -> dict[str, Any]:
    """Run local ingestion and deterministic silver/gold reconstruction."""
    manifest_path = paths.bronze / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError("bronze manifest not found; run the generate command first")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paths.state.mkdir(parents=True, exist_ok=True)
    paths.evidence.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)
    connection = duckdb.connect(str(paths.database))
    _create_control_tables(connection)
    source_files = sorted(paths.bronze.glob("source/**/*.parquet"))
    connection.execute(
        "INSERT INTO meta_pipeline_runs VALUES (?, ?, NULL, 'RUNNING', ?, 0, 0, NULL)",
        [run_id, started_at, len(source_files)],
    )
    loaded = 0
    skipped = 0
    try:
        expected_hashes = {item["path"]: item["sha256"] for item in manifest["files"]}
        actual_relatives = {path.relative_to(paths.bronze).as_posix() for path in source_files}
        if actual_relatives != set(expected_hashes):
            raise ValueError("source file set differs from manifest")
        for path in source_files:
            relative = path.relative_to(paths.bronze).as_posix()
            kind = _file_kind(relative)
            file_hash = _sha256(path)
            if file_hash != expected_hashes[relative]:
                raise ValueError(f"hash mismatch for {relative}")
            _validate_parquet_schema(path, kind)
            existing = connection.execute(
                "SELECT sha256, status FROM meta_ingestion_files WHERE source_file = ?", [relative]
            ).fetchone()
            if existing == (file_hash, "SUCCESS"):
                skipped += 1
                continue
            _load_file(connection, path, relative, kind, file_hash, run_id)
            loaded += 1

        connection.execute(
            "CREATE OR REPLACE TEMP TABLE current_run AS SELECT ?::VARCHAR AS run_id", [run_id]
        )
        _execute_sql_file(connection, "transform/rebuild_silver.sql")
        _execute_sql_file(connection, "transform/rebuild_gold.sql")
        _execute_sql_file(connection, "tests/assertions.sql")
        _export_tables(connection, paths)
        counts = _table_counts(connection)
        completed_at = datetime.now(UTC)
        connection.execute(
            """
            UPDATE meta_pipeline_runs
            SET completed_at = ?, status = 'SUCCESS', loaded_files = ?, skipped_files = ?
            WHERE run_id = ?
            """,
            [completed_at, loaded, skipped, run_id],
        )
        quality_counts = dict(
            connection.execute(
                "SELECT rule_code, failed_record_count FROM fact_data_quality ORDER BY rule_code"
            ).fetchall()
        )
        summary: dict[str, Any] = {
            "run_id": run_id,
            "status": "SUCCESS",
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "source_manifest_seed": manifest["seed"],
            "source_shipment_count": manifest["shipment_count"],
            "expected_injected_counts": manifest["expected_counts"],
            "discovered_files": len(source_files),
            "loaded_files": loaded,
            "skipped_files": skipped,
            "table_counts": counts,
            "quality_counts": quality_counts,
            "execution_scope": "LOCAL_DUCKDB_PARQUET",
            "fabric_verified": False,
            "power_bi_verified": False,
        }
        (paths.evidence / "latest_run.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return summary
    except Exception as exc:
        connection.execute(
            """
            UPDATE meta_pipeline_runs
            SET completed_at = current_timestamp, status = 'FAILED', loaded_files = ?,
                skipped_files = ?, error_message = ?
            WHERE run_id = ?
            """,
            [loaded, skipped, str(exc)[:2000], run_id],
        )
        raise
    finally:
        connection.close()
