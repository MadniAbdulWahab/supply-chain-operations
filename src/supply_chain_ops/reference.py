from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from supply_chain_ops.config import ProjectPaths

RECONCILED_TABLES = [
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

QUALITY_RULES = [
    "DQ_SHP_001_DUPLICATE_ID",
    "DQ_SHP_002_REQUIRED_FIELD",
    "DQ_SHP_003_WEIGHT_RANGE",
    "DQ_SHP_004_UNKNOWN_ROUTE",
    "DQ_SHP_005_UNKNOWN_HUB",
    "DQ_SHP_006_UNKNOWN_SERVICE",
    "DQ_SHP_007_ROUTE_HUB_MISMATCH",
    "DQ_SHP_008_INVALID_PROMISE",
    "DQ_EVT_001_DUPLICATE_ID",
    "DQ_EVT_002_REQUIRED_FIELD",
    "DQ_EVT_003_UNKNOWN_SHIPMENT",
    "DQ_EVT_004_INVALID_PARENT_SHIPMENT",
    "DQ_EVT_005_UNKNOWN_HUB",
    "DQ_EVT_006_INVALID_SEQUENCE",
    "DQ_EVT_007_BEFORE_SHIPMENT",
]

# Each query is written in the SQL subset shared by DuckDB and Spark SQL, so the Fabric
# notebook can run the same text against the Delta tables.
KPI_QUERIES: dict[str, tuple[str, float]] = {
    "delivered_shipments": (
        "SELECT count(*) FROM fact_shipments WHERE actual_delivery_ts IS NOT NULL",
        0,
    ),
    "on_time_shipments": ("SELECT count(*) FROM fact_shipments WHERE is_on_time", 0),
    "total_delay_minutes": ("SELECT sum(delay_minutes) FROM fact_shipments", 0),
    "total_transit_hours": ("SELECT sum(transit_hours) FROM fact_shipments", 0.01),
    "total_weight_kg": ("SELECT sum(weight_kg) FROM fact_shipments", 0.01),
    "late_arriving_events": ("SELECT count(*) FROM fact_events WHERE is_late_arriving", 0),
    "total_arrival_lag_days": ("SELECT sum(arrival_lag_days) FROM fact_events", 0),
    "late_events_on_shipments": (
        "SELECT sum(late_arriving_event_count) FROM fact_shipments",
        0,
    ),
    "route_key_checksum": ("SELECT sum(route_key) FROM fact_shipments", 0),
    "event_hub_key_checksum": ("SELECT sum(hub_key) FROM fact_events", 0),
    "first_date_key": ("SELECT min(date_key) FROM dim_date", 0),
    "last_date_key": ("SELECT max(date_key) FROM dim_date", 0),
    "weekend_days": ("SELECT count(*) FROM dim_date WHERE is_weekend", 0),
}


def reconciliation_checks() -> dict[str, dict[str, Any]]:
    checks: dict[str, dict[str, Any]] = {}
    for table in RECONCILED_TABLES:
        checks[f"rows.{table}"] = {"sql": f"SELECT count(*) FROM {table}", "tolerance": 0}
    for rule in QUALITY_RULES:
        checks[f"quality.{rule}"] = {
            "sql": f"SELECT count(*) FROM quarantine_records WHERE rule_code = '{rule}'",
            "tolerance": 0,
        }
    for name, (sql, tolerance) in KPI_QUERIES.items():
        checks[f"kpi.{name}"] = {"sql": sql, "tolerance": tolerance}
    return checks


def _plain(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, float):
        return value
    return int(value)


def build_reference(paths: ProjectPaths) -> dict[str, Any]:
    """Export the local DuckDB results that the Fabric run must reproduce."""
    if not paths.database.exists():
        raise FileNotFoundError("local lakehouse not found; run the pipeline first")
    manifest = json.loads((paths.bronze / "manifest.json").read_text(encoding="utf-8"))
    checks = reconciliation_checks()
    with duckdb.connect(str(paths.database), read_only=True) as connection:
        for check in checks.values():
            check["expected"] = _plain(connection.execute(check["sql"]).fetchone()[0])
    reference = {
        "source": "LOCAL_DUCKDB_PARQUET",
        "source_manifest_seed": manifest["seed"],
        "source_shipment_count": manifest["shipment_count"],
        "checks": checks,
    }
    paths.evidence.mkdir(parents=True, exist_ok=True)
    destination: Path = paths.evidence / "local_reference.json"
    destination.write_text(json.dumps(reference, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return reference
