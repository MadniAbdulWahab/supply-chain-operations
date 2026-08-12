-- Dimensional gold model. Each fact declares one row per business grain.
CREATE OR REPLACE TABLE dim_hub AS
SELECT
    row_number() OVER (ORDER BY hub_id)::INTEGER AS hub_key,
    hub_id, hub_name, city, state_code
FROM bronze_hubs;

CREATE OR REPLACE TABLE dim_route AS
SELECT
    row_number() OVER (ORDER BY route_id)::INTEGER AS route_key,
    route_id, origin_hub_id, destination_hub_id, distance_km,
    origin_hub_id || ' -> ' || destination_hub_id AS route_label
FROM bronze_routes;

CREATE OR REPLACE TABLE dim_service_level AS
SELECT
    row_number() OVER (ORDER BY service_level_id)::INTEGER AS service_level_key,
    service_level_id, service_level_name, sla_days, base_price_eur
FROM bronze_service_levels;

CREATE OR REPLACE TABLE dim_date AS
WITH bounds AS (
    SELECT
        least(min(CAST(created_ts AS DATE)), (SELECT min(CAST(event_ts AS DATE)) FROM silver_events)) AS min_date,
        greatest(max(CAST(promised_delivery_ts AS DATE)), (SELECT max(CAST(event_ts AS DATE)) FROM silver_events)) AS max_date
    FROM silver_shipments
), dates AS (
    SELECT unnest(generate_series(min_date, max_date, INTERVAL 1 DAY))::DATE AS calendar_date
    FROM bounds
)
SELECT
    CAST(strftime(calendar_date, '%Y%m%d') AS INTEGER) AS date_key,
    calendar_date,
    year(calendar_date)::SMALLINT AS calendar_year,
    quarter(calendar_date)::SMALLINT AS calendar_quarter,
    month(calendar_date)::SMALLINT AS calendar_month,
    strftime(calendar_date, '%B') AS month_name,
    weekofyear(calendar_date)::SMALLINT AS iso_week,
    dayofweek(calendar_date)::SMALLINT AS day_of_week,
    strftime(calendar_date, '%A') AS day_name,
    dayofweek(calendar_date) IN (0, 6) AS is_weekend
FROM dates;

CREATE OR REPLACE TABLE fact_shipments AS
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
    CAST(strftime(CAST(s.created_ts AS DATE), '%Y%m%d') AS INTEGER) AS created_date_key,
    CAST(strftime(CAST(s.promised_delivery_ts AS DATE), '%Y%m%d') AS INTEGER) AS promised_date_key,
    CASE WHEN er.actual_delivery_ts IS NOT NULL
        THEN CAST(strftime(CAST(er.actual_delivery_ts AS DATE), '%Y%m%d') AS INTEGER) END AS delivered_date_key,
    oh.hub_key AS origin_hub_key,
    dh.hub_key AS destination_hub_key,
    r.route_key,
    sl.service_level_key,
    s.customer_region,
    s.weight_kg,
    s.created_ts,
    s.promised_delivery_ts,
    er.actual_delivery_ts,
    date_diff('minute', s.created_ts, er.actual_delivery_ts) / 60.0 AS transit_hours,
    date_diff('minute', s.promised_delivery_ts, er.actual_delivery_ts) AS delay_minutes,
    er.actual_delivery_ts IS NOT NULL AND er.actual_delivery_ts <= s.promised_delivery_ts AS is_on_time,
    er.latest_event_type AS current_status,
    coalesce(er.valid_event_count, 0) AS valid_event_count,
    coalesce(er.late_arriving_event_count, 0) AS late_arriving_event_count
FROM silver_shipments s
JOIN dim_hub oh ON s.origin_hub_id = oh.hub_id
JOIN dim_hub dh ON s.destination_hub_id = dh.hub_id
JOIN dim_route r ON s.route_id = r.route_id
JOIN dim_service_level sl ON s.service_level_id = sl.service_level_id
LEFT JOIN event_rollup er ON s.shipment_id = er.shipment_id;

CREATE OR REPLACE TABLE fact_events AS
SELECT
    e.event_id,
    e.shipment_id,
    CAST(strftime(CAST(e.event_ts AS DATE), '%Y%m%d') AS INTEGER) AS event_date_key,
    h.hub_key,
    e.event_sequence,
    e.event_type,
    e.event_ts,
    e.arrival_batch_date,
    e.arrival_lag_days,
    e.arrival_lag_days >= 2 AS is_late_arriving
FROM silver_events e
JOIN dim_hub h ON e.hub_id = h.hub_id;

CREATE OR REPLACE TABLE fact_data_quality AS
SELECT
    (SELECT run_id FROM current_run) AS evaluation_run_id,
    current_timestamp AS evaluated_at,
    rule_code,
    record_type,
    count(*) AS failed_record_count,
    count(DISTINCT source_file) AS affected_file_count
FROM quarantine_records
GROUP BY rule_code, record_type;

