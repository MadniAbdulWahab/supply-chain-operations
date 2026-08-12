-- Deterministic local silver reconstruction. DuckDB dialect.
CREATE OR REPLACE TABLE silver_shipments_evaluated AS
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY shipment_id ORDER BY source_file, source_row
    ) AS duplicate_rank
    FROM bronze_shipments
), evaluated AS (
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
)
SELECT * FROM evaluated;

CREATE OR REPLACE TABLE silver_shipments AS
SELECT
    shipment_id, created_ts, promised_delivery_ts, origin_hub_id, destination_hub_id,
    route_id, service_level_id, customer_region, weight_kg,
    source_file, source_sha256, source_row, ingestion_run_id, ingested_at
FROM silver_shipments_evaluated
WHERE rejection_rule IS NULL;

CREATE OR REPLACE TABLE silver_events_evaluated AS
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY event_id ORDER BY source_file, source_row
    ) AS duplicate_rank
    FROM bronze_events
), evaluated AS (
    SELECT e.*,
        CAST(regexp_extract(e.source_file, 'batch_date=([0-9-]+)', 1) AS DATE) AS arrival_batch_date,
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
)
SELECT * FROM evaluated;

CREATE OR REPLACE TABLE silver_events AS
SELECT
    event_id, shipment_id, event_sequence, event_type, event_ts, hub_id,
    arrival_batch_date,
    date_diff('day', CAST(event_ts AS DATE), arrival_batch_date) AS arrival_lag_days,
    source_file, source_sha256, source_row, ingestion_run_id, ingested_at
FROM silver_events_evaluated
WHERE rejection_rule IS NULL;

CREATE OR REPLACE TABLE quarantine_records AS
SELECT
    'shipment' AS record_type,
    coalesce(shipment_id, '<NULL>') AS record_key,
    rejection_rule AS rule_code,
    to_json(struct_pack(
        shipment_id := shipment_id,
        created_ts := created_ts,
        promised_delivery_ts := promised_delivery_ts,
        route_id := route_id,
        weight_kg := weight_kg
    )) AS raw_record_json,
    source_file, source_sha256, source_row,
    (SELECT run_id FROM current_run) AS evaluation_run_id,
    current_timestamp AS quarantined_at
FROM silver_shipments_evaluated
WHERE rejection_rule IS NOT NULL
UNION ALL
SELECT
    'event' AS record_type,
    coalesce(event_id, '<NULL>') AS record_key,
    rejection_rule AS rule_code,
    to_json(struct_pack(
        event_id := event_id,
        shipment_id := shipment_id,
        event_sequence := event_sequence,
        event_type := event_type,
        event_ts := event_ts,
        hub_id := hub_id
    )) AS raw_record_json,
    source_file, source_sha256, source_row,
    (SELECT run_id FROM current_run) AS evaluation_run_id,
    current_timestamp AS quarantined_at
FROM silver_events_evaluated
WHERE rejection_rule IS NOT NULL;

