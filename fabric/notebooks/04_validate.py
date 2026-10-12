# Fabric notebook: SQL assertions and reconciliation with the local DuckDB run
# Spark SQL port of sql/tests/assertions.sql plus the checks exported by
# `python -m supply_chain_ops reference`.

# %% tags=["parameters"]
run_id = ""
reference_path = "Files/reference/local_reference.json"
execution_scope = "FABRIC_SPARK_DELTA"

# %%
import json
import os
from datetime import datetime, timezone

spark.conf.set("spark.sql.session.timeZone", "UTC")
RUN_SCHEMA = (
    "run_id STRING, started_at TIMESTAMP, completed_at TIMESTAMP, status STRING, "
    "stage STRING, discovered_files BIGINT, loaded_files BIGINT, skipped_files BIGINT, "
    "error_message STRING"
)
RUN_FIELDS = [part.split()[0] for part in RUN_SCHEMA.split(", ")]
if not run_id:
    run_id = spark.sql(
        "SELECT run_id FROM meta_pipeline_runs ORDER BY started_at DESC LIMIT 1"
    ).first()[0]


def update_run(**changes):
    record = spark.table("meta_pipeline_runs").where(f"run_id = '{run_id}'").first().asDict()
    record.update(changes)
    row = tuple(record[field] for field in RUN_FIELDS)
    spark.createDataFrame([row], RUN_SCHEMA).createOrReplaceTempView("run_update")
    spark.sql(
        """
        MERGE INTO meta_pipeline_runs AS t USING run_update AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET *
        """
    )
    return record


# Each query returns the number of violating rows; zero passes.
ASSERTIONS = {
    "silver_shipments shipment_id is unique": """
        SELECT count(*) - count(DISTINCT shipment_id) FROM silver_shipments
    """,
    "silver_events event_id is unique": """
        SELECT count(*) - count(DISTINCT event_id) FROM silver_events
    """,
    "silver_shipments has valid references": """
        SELECT count(*)
        FROM silver_shipments s
        LEFT JOIN bronze_routes r ON s.route_id = r.route_id
        LEFT JOIN bronze_hubs oh ON s.origin_hub_id = oh.hub_id
        LEFT JOIN bronze_hubs dh ON s.destination_hub_id = dh.hub_id
        LEFT JOIN bronze_service_levels sl ON s.service_level_id = sl.service_level_id
        WHERE r.route_id IS NULL OR oh.hub_id IS NULL OR dh.hub_id IS NULL
           OR sl.service_level_id IS NULL
    """,
    "silver_events has no orphan references": """
        SELECT count(*)
        FROM silver_events e
        LEFT JOIN silver_shipments s ON e.shipment_id = s.shipment_id
        LEFT JOIN bronze_hubs h ON e.hub_id = h.hub_id
        WHERE s.shipment_id IS NULL OR h.hub_id IS NULL
    """,
    "fact_shipments has one row per shipment": """
        SELECT count(*) FROM (
            SELECT shipment_id FROM fact_shipments GROUP BY shipment_id HAVING count(*) > 1
        )
    """,
    "on-time flag agrees with delivery and promise timestamps": """
        SELECT count(*) FROM fact_shipments
        WHERE is_on_time <> (actual_delivery_ts IS NOT NULL
                             AND actual_delivery_ts <= promised_delivery_ts)
    """,
    "quarantine rule codes are documented": """
        SELECT count(*) FROM quarantine_records WHERE rule_code IS NULL OR trim(rule_code) = ''
    """,
}

# %%
try:
    update_run(stage="validate")
    with open(os.path.join("/lakehouse/default", reference_path), encoding="utf-8") as handle:
        reference = json.load(handle)

    checked_at = datetime.now(timezone.utc)
    results = []
    for name, sql in ASSERTIONS.items():
        violations = spark.sql(sql).first()[0]
        results.append(("assertion", name, 0.0, float(violations), 0.0, violations == 0))
    for name, check in sorted(reference["checks"].items()):
        actual = spark.sql(check["sql"]).first()[0]
        expected = check["expected"]
        if expected is None or actual is None:
            passed = expected is None and actual is None
        else:
            passed = abs(float(actual) - float(expected)) <= check["tolerance"]
        results.append(
            (
                "reconciliation",
                name,
                None if expected is None else float(expected),
                None if actual is None else float(actual),
                float(check["tolerance"]),
                passed,
            )
        )

    spark.createDataFrame(
        [(run_id, checked_at, *result) for result in results],
        "run_id STRING, checked_at TIMESTAMP, check_type STRING, check_name STRING, "
        "expected_value DOUBLE, actual_value DOUBLE, tolerance DOUBLE, passed BOOLEAN",
    ).write.format("delta").mode("append").saveAsTable("meta_reconciliation")

    failures = [result for result in results if not result[-1]]
    status = "SUCCESS" if not failures else "FAILED"
    message = None if not failures else f"{len(failures)} checks failed: " + ", ".join(
        result[1] for result in failures
    )
    record = update_run(
        status=status, completed_at=datetime.now(timezone.utc), error_message=message
    )
except Exception as exc:
    update_run(
        status="FAILED", completed_at=datetime.now(timezone.utc), error_message=str(exc)[:2000]
    )
    raise

summary = {
    "run_id": run_id,
    "status": status,
    "execution_scope": execution_scope,
    "spark_version": spark.version,
    "started_at": record["started_at"].isoformat(),
    "completed_at": record["completed_at"].isoformat(),
    "loaded_files": record["loaded_files"],
    "skipped_files": record["skipped_files"],
    "assertions_passed": sum(1 for r in results if r[0] == "assertion" and r[-1]),
    "assertions_total": sum(1 for r in results if r[0] == "assertion"),
    "reconciliation_passed": sum(1 for r in results if r[0] == "reconciliation" and r[-1]),
    "reconciliation_total": sum(1 for r in results if r[0] == "reconciliation"),
    "reference_source": reference["source"],
    "failed_checks": [
        {"check": r[1], "expected": r[2], "actual": r[3]} for r in failures
    ],
}
evidence_dir = "/lakehouse/default/Files/evidence"
os.makedirs(evidence_dir, exist_ok=True)
with open(os.path.join(evidence_dir, f"run_{run_id}.json"), "w", encoding="utf-8") as handle:
    json.dump(summary, handle, indent=2, sort_keys=True)
print(json.dumps(summary, indent=2, sort_keys=True))
if failures:
    raise AssertionError(message)
