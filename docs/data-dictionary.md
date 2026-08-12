# Data dictionary

## Source and silver entities

### `shipments` / `silver_shipments`

Grain: one row per shipment.

| Column | Type | Meaning |
|---|---|---|
| `shipment_id` | string | Deterministic synthetic business identifier |
| `created_ts` | timestamp | Shipment creation time |
| `promised_delivery_ts` | timestamp | Customer delivery commitment |
| `origin_hub_id` | string | Origin hub natural key |
| `destination_hub_id` | string | Destination hub natural key |
| `route_id` | string | Planned route natural key |
| `service_level_id` | string | Express, Standard, or Economy key |
| `customer_region` | string | Synthetic reporting segment |
| `weight_kg` | decimal-like double | Shipment weight in kilograms |

Silver adds source file/hash/row, ingestion run/time. Invalid rows are excluded from silver and
represented in `quarantine_records`.

### `events` / `silver_events`

Grain: one scan event per `event_id` after deduplication.

| Column | Type | Meaning |
|---|---|---|
| `event_id` | string | Deterministic event identifier |
| `shipment_id` | string | Parent shipment key |
| `event_sequence` | integer | Expected lifecycle position, 1 through 5 |
| `event_type` | string | CREATED through DELIVERED |
| `event_ts` | timestamp | Business event time |
| `hub_id` | string | Hub where the event occurred |
| `arrival_batch_date` | date | Date the batch arrived at the platform |
| `arrival_lag_days` | integer | Batch date minus business event date |

## Gold facts

### `fact_shipments`

Grain: one row per valid shipment. Contains dimension keys, promise and actual timestamps,
delivery/transit measures, status, valid scan count, and late-arriving scan count.

### `fact_events`

Grain: one valid, deduplicated scan event. Supports lifecycle, volume, hub, and late-arrival
analysis.

### `fact_data_quality`

Grain: one evaluation run, entity type, and failed rule. Stores failed records and affected
source-file counts.

## Gold dimensions

- `dim_date`: one row per calendar day spanning business timestamps.
- `dim_hub`: one row per fictional network hub.
- `dim_route`: one row per directional route.
- `dim_service_level`: one row per delivery service promise.

