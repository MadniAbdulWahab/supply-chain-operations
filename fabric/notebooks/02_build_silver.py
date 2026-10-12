# Fabric notebook: silver quality processing
# Spark SQL port of sql/transform/rebuild_silver.sql (first failing rule wins).

# %% tags=["parameters"]
run_id = ""

# %%
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
spark.createDataFrame([(run_id,)], "run_id STRING").createOrReplaceTempView("current_run")


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


# %%
SILVER_STATEMENTS = [
    """
    CREATE OR REPLACE TEMP VIEW silver_shipments_evaluated AS
    WITH ranked AS (
        SELECT *, row_number() OVER (
            PARTITION BY shipment_id ORDER BY source_file, source_row
        ) AS duplicate_rank
        FROM bronze_shipments
    )
    SELECT s.*,
        CASE
            WHEN s.duplicate_rank > 1 THEN 'DQ_SHP_001_DUPLICATE_ID'
            WHEN s.shipment_id IS NULL OR s.created_ts IS NULL OR s.promised_delivery_ts IS NULL
                OR s.origin_hub_id IS NULL OR s.destination_hub_id IS NULL
                OR s.route_id IS NULL OR s.service_level_id IS NULL
                THEN 'DQ_SHP_002_REQUIRED_FIELD'
            WHEN s.weight_kg IS NULL OR s.weight_kg <= 0 OR s.weight_kg > 40
                THEN 'DQ_SHP_003_WEIGHT_RANGE'
            WHEN r.route_id IS NULL THEN 'DQ_SHP_004_UNKNOWN_ROUTE'
            WHEN oh.hub_id IS NULL OR dh.hub_id IS NULL THEN 'DQ_SHP_005_UNKNOWN_HUB'
            WHEN sl.service_level_id IS NULL THEN 'DQ_SHP_006_UNKNOWN_SERVICE'
            WHEN r.origin_hub_id <> s.origin_hub_id OR r.destination_hub_id <> s.destination_hub_id
                THEN 'DQ_SHP_007_ROUTE_HUB_MISMATCH'
            WHEN s.promised_delivery_ts <= s.created_ts THEN 'DQ_SHP_008_INVALID_PROMISE'
        END AS rejection_rule
    FROM ranked s
    LEFT JOIN bronze_routes r ON s.route_id = r.route_id
    LEFT JOIN bronze_hubs oh ON s.origin_hub_id = oh.hub_id
    LEFT JOIN bronze_hubs dh ON s.destination_hub_id = dh.hub_id
    LEFT JOIN bronze_service_levels sl ON s.service_level_id = sl.service_level_id
    """,
    "CACHE TABLE silver_shipments_evaluated",
    """
    CREATE OR REPLACE TABLE silver_shipments USING DELTA AS
    SELECT
        shipment_id, created_ts, promised_delivery_ts, origin_hub_id, destination_hub_id,
        route_id, service_level_id, customer_region, weight_kg,
        source_file, source_sha256, source_row, ingestion_run_id, ingested_at
    FROM silver_shipments_evaluated
    WHERE rejection_rule IS NULL
    """,
    """
    CREATE OR REPLACE TEMP VIEW silver_events_evaluated AS
    WITH ranked AS (
        SELECT *, row_number() OVER (
            PARTITION BY event_id ORDER BY source_file, source_row
        ) AS duplicate_rank
        FROM bronze_events
    )
    SELECT e.*,
        CAST(nullif(regexp_extract(e.source_file, 'batch_date=([0-9-]+)', 1), '') AS DATE)
            AS arrival_batch_date,
        CASE
            WHEN e.duplicate_rank > 1 THEN 'DQ_EVT_001_DUPLICATE_ID'
            WHEN e.event_id IS NULL OR e.shipment_id IS NULL OR e.event_sequence IS NULL
                OR e.event_type IS NULL OR e.event_ts IS NULL OR e.hub_id IS NULL
                THEN 'DQ_EVT_002_REQUIRED_FIELD'
            WHEN bs.shipment_id IS NULL THEN 'DQ_EVT_003_UNKNOWN_SHIPMENT'
            WHEN ss.shipment_id IS NULL THEN 'DQ_EVT_004_INVALID_PARENT_SHIPMENT'
            WHEN h.hub_id IS NULL THEN 'DQ_EVT_005_UNKNOWN_HUB'
            WHEN e.event_sequence NOT BETWEEN 1 AND 5
                OR e.event_type <> CASE e.event_sequence
                    WHEN 1 THEN 'CREATED'
                    WHEN 2 THEN 'PICKED_UP'
                    WHEN 3 THEN 'DEPARTED_ORIGIN'
                    WHEN 4 THEN 'ARRIVED_DESTINATION'
                    WHEN 5 THEN 'DELIVERED'
                END THEN 'DQ_EVT_006_INVALID_SEQUENCE'
            WHEN e.event_ts < bs.created_ts THEN 'DQ_EVT_007_BEFORE_SHIPMENT'
        END AS rejection_rule
    FROM ranked e
    LEFT JOIN bronze_shipments bs ON e.shipment_id = bs.shipment_id
    LEFT JOIN silver_shipments ss ON e.shipment_id = ss.shipment_id
    LEFT JOIN bronze_hubs h ON e.hub_id = h.hub_id
    """,
    "CACHE TABLE silver_events_evaluated",
    """
    CREATE OR REPLACE TABLE silver_events USING DELTA AS
    SELECT
        event_id, shipment_id, event_sequence, event_type, event_ts, hub_id,
        arrival_batch_date,
        datediff(arrival_batch_date, CAST(event_ts AS DATE)) AS arrival_lag_days,
        source_file, source_sha256, source_row, ingestion_run_id, ingested_at
    FROM silver_events_evaluated
    WHERE rejection_rule IS NULL
    """,
    """
    CREATE OR REPLACE TABLE quarantine_records USING DELTA AS
    SELECT
        'shipment' AS record_type,
        coalesce(shipment_id, '<NULL>') AS record_key,
        rejection_rule AS rule_code,
        to_json(named_struct(
            'shipment_id', shipment_id,
            'created_ts', created_ts,
            'promised_delivery_ts', promised_delivery_ts,
            'route_id', route_id,
            'weight_kg', weight_kg
        ), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss')) AS raw_record_json,
        source_file, source_sha256, source_row,
        (SELECT run_id FROM current_run) AS evaluation_run_id,
        current_timestamp() AS quarantined_at
    FROM silver_shipments_evaluated
    WHERE rejection_rule IS NOT NULL
    UNION ALL
    SELECT
        'event' AS record_type,
        coalesce(event_id, '<NULL>') AS record_key,
        rejection_rule AS rule_code,
        to_json(named_struct(
            'event_id', event_id,
            'shipment_id', shipment_id,
            'event_sequence', event_sequence,
            'event_type', event_type,
            'event_ts', event_ts,
            'hub_id', hub_id
        ), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss')) AS raw_record_json,
        source_file, source_sha256, source_row,
        (SELECT run_id FROM current_run) AS evaluation_run_id,
        current_timestamp() AS quarantined_at
    FROM silver_events_evaluated
    WHERE rejection_rule IS NOT NULL
    """,
]

# %%
try:
    update_run(stage="silver")
    for statement in SILVER_STATEMENTS:
        spark.sql(statement)
except Exception as exc:
    update_run(
        status="FAILED", completed_at=datetime.now(timezone.utc), error_message=str(exc)[:2000]
    )
    raise
finally:
    spark.catalog.clearCache()

for table in ["silver_shipments", "silver_events", "quarantine_records"]:
    print(table, spark.table(table).count())
