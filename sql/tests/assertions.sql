-- Executable SQL data tests. Every failing query triggers a DuckDB error.
SELECT CASE WHEN count(*) = count(DISTINCT shipment_id) THEN 1
    ELSE error('silver_shipments shipment_id is not unique') END
FROM silver_shipments;

SELECT CASE WHEN count(*) = count(DISTINCT event_id) THEN 1
    ELSE error('silver_events event_id is not unique') END
FROM silver_events;

SELECT CASE WHEN count(*) = 0 THEN 1
    ELSE error('silver_shipments contains invalid references') END
FROM silver_shipments s
LEFT JOIN bronze_routes r USING (route_id)
LEFT JOIN bronze_hubs oh ON s.origin_hub_id = oh.hub_id
LEFT JOIN bronze_hubs dh ON s.destination_hub_id = dh.hub_id
LEFT JOIN bronze_service_levels sl USING (service_level_id)
WHERE r.route_id IS NULL OR oh.hub_id IS NULL OR dh.hub_id IS NULL
   OR sl.service_level_id IS NULL;

SELECT CASE WHEN count(*) = 0 THEN 1
    ELSE error('silver_events contains orphan references') END
FROM silver_events e
LEFT JOIN silver_shipments s USING (shipment_id)
LEFT JOIN bronze_hubs h USING (hub_id)
WHERE s.shipment_id IS NULL OR h.hub_id IS NULL;

SELECT CASE WHEN count(*) = 0 THEN 1
    ELSE error('fact_shipments grain is not one row per shipment') END
FROM (
    SELECT shipment_id FROM fact_shipments GROUP BY shipment_id HAVING count(*) > 1
);

SELECT CASE WHEN count(*) = 0 THEN 1
    ELSE error('on-time flag contradicts delivery and promise timestamps') END
FROM fact_shipments
WHERE is_on_time <> (actual_delivery_ts IS NOT NULL AND actual_delivery_ts <= promised_delivery_ts);

SELECT CASE WHEN count(*) = 0 THEN 1
    ELSE error('quarantine contains an undocumented empty rule code') END
FROM quarantine_records
WHERE rule_code IS NULL OR trim(rule_code) = '';

