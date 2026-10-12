# Fabric notebook: dimensional gold model
# Spark SQL port of sql/transform/rebuild_gold.sql. Each fact keeps the same declared grain.

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


def minutes(timestamp_sql):
    # DuckDB's date_diff('minute', a, b) counts minute boundaries crossed, not whole minutes.
    return f"floor(unix_timestamp({timestamp_sql}) / 60)"


# %%
GOLD_STATEMENTS = [
    """
    CREATE OR REPLACE TABLE dim_hub USING DELTA AS
    SELECT
        CAST(row_number() OVER (ORDER BY hub_id) AS INT) AS hub_key,
        hub_id, hub_name, city, state_code
    FROM bronze_hubs
    """,
    """
    CREATE OR REPLACE TABLE dim_route USING DELTA AS
    SELECT
        CAST(row_number() OVER (ORDER BY route_id) AS INT) AS route_key,
        route_id, origin_hub_id, destination_hub_id, distance_km,
        origin_hub_id || ' -> ' || destination_hub_id AS route_label
    FROM bronze_routes
    """,
    """
    CREATE OR REPLACE TABLE dim_service_level USING DELTA AS
    SELECT
        CAST(row_number() OVER (ORDER BY service_level_id) AS INT) AS service_level_key,
        service_level_id, service_level_name, sla_days, base_price_eur
    FROM bronze_service_levels
    """,
    """
    CREATE OR REPLACE TABLE dim_date USING DELTA AS
    WITH day_values AS (
        SELECT CAST(created_ts AS DATE) AS day_value FROM silver_shipments
        UNION ALL
        SELECT CAST(promised_delivery_ts AS DATE) FROM silver_shipments
        UNION ALL
        SELECT CAST(event_ts AS DATE) FROM silver_events
    ), bounds AS (
        SELECT min(day_value) AS min_date, max(day_value) AS max_date FROM day_values
    ), dates AS (
        SELECT explode(sequence(min_date, max_date, INTERVAL 1 DAY)) AS calendar_date
        FROM bounds
    )
    SELECT
        CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
        calendar_date,
        CAST(year(calendar_date) AS SMALLINT) AS calendar_year,
        CAST(quarter(calendar_date) AS SMALLINT) AS calendar_quarter,
        CAST(month(calendar_date) AS SMALLINT) AS calendar_month,
        date_format(calendar_date, 'MMMM') AS month_name,
        CAST(weekofyear(calendar_date) AS SMALLINT) AS iso_week,
        CAST(dayofweek(calendar_date) - 1 AS SMALLINT) AS day_of_week,
        date_format(calendar_date, 'EEEE') AS day_name,
        dayofweek(calendar_date) IN (1, 7) AS is_weekend
    FROM dates
    """,
    f"""
    CREATE OR REPLACE TABLE fact_shipments USING DELTA AS
    WITH event_rollup AS (
        SELECT
            shipment_id,
            max(event_ts) FILTER (WHERE event_type = 'DELIVERED') AS actual_delivery_ts,
            max_by(event_type, event_ts) AS latest_event_type,
            count(*) AS valid_event_count,
            count(*) FILTER (WHERE arrival_lag_days >= 2) AS late_arriving_event_count
        FROM silver_events
        GROUP BY shipment_id
    )
    SELECT
        s.shipment_id,
        CAST(date_format(s.created_ts, 'yyyyMMdd') AS INT) AS created_date_key,
        CAST(date_format(s.promised_delivery_ts, 'yyyyMMdd') AS INT) AS promised_date_key,
        CAST(date_format(er.actual_delivery_ts, 'yyyyMMdd') AS INT) AS delivered_date_key,
        oh.hub_key AS origin_hub_key,
        dh.hub_key AS destination_hub_key,
        r.route_key,
        sl.service_level_key,
        s.customer_region,
        s.weight_kg,
        s.created_ts,
        s.promised_delivery_ts,
        er.actual_delivery_ts,
        CAST({minutes("er.actual_delivery_ts")} - {minutes("s.created_ts")} AS DOUBLE) / 60
            AS transit_hours,
        CAST({minutes("er.actual_delivery_ts")} - {minutes("s.promised_delivery_ts")} AS BIGINT)
            AS delay_minutes,
        er.actual_delivery_ts IS NOT NULL AND er.actual_delivery_ts <= s.promised_delivery_ts
            AS is_on_time,
        er.latest_event_type AS current_status,
        coalesce(er.valid_event_count, 0) AS valid_event_count,
        coalesce(er.late_arriving_event_count, 0) AS late_arriving_event_count
    FROM silver_shipments s
    JOIN dim_hub oh ON s.origin_hub_id = oh.hub_id
    JOIN dim_hub dh ON s.destination_hub_id = dh.hub_id
    JOIN dim_route r ON s.route_id = r.route_id
    JOIN dim_service_level sl ON s.service_level_id = sl.service_level_id
    LEFT JOIN event_rollup er ON s.shipment_id = er.shipment_id
    """,
    """
    CREATE OR REPLACE TABLE fact_events USING DELTA AS
    SELECT
        e.event_id,
        e.shipment_id,
        CAST(date_format(e.event_ts, 'yyyyMMdd') AS INT) AS event_date_key,
        h.hub_key,
        e.event_sequence,
        e.event_type,
        e.event_ts,
        e.arrival_batch_date,
        e.arrival_lag_days,
        e.arrival_lag_days >= 2 AS is_late_arriving
    FROM silver_events e
    JOIN dim_hub h ON e.hub_id = h.hub_id
    """,
    """
    CREATE OR REPLACE TABLE fact_data_quality USING DELTA AS
    SELECT
        (SELECT run_id FROM current_run) AS evaluation_run_id,
        current_timestamp() AS evaluated_at,
        rule_code,
        record_type,
        count(*) AS failed_record_count,
        count(DISTINCT source_file) AS affected_file_count
    FROM quarantine_records
    GROUP BY rule_code, record_type
    """,
]

# %%
try:
    update_run(stage="gold")
    for statement in GOLD_STATEMENTS:
        spark.sql(statement)
except Exception as exc:
    update_run(
        status="FAILED", completed_at=datetime.now(timezone.utc), error_message=str(exc)[:2000]
    )
    raise

for table in [
    "dim_date", "dim_hub", "dim_route", "dim_service_level",
    "fact_shipments", "fact_events", "fact_data_quality",
]:
    print(table, spark.table(table).count())
