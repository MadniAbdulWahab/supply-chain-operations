# Data-quality rules

Rules use first-failure precedence so a bad row is counted once in quarantine. Injection counts
describe the deliberately changed source records; observed counts may also include downstream
impact such as events whose parent shipment was rejected.

| Rule code | Entity | Check | Action |
|---|---|---|---|
| `DQ_SHP_001_DUPLICATE_ID` | Shipment | Repeated shipment ID | Keep first, quarantine later rows |
| `DQ_SHP_002_REQUIRED_FIELD` | Shipment | Required value is null | Quarantine |
| `DQ_SHP_003_WEIGHT_RANGE` | Shipment | Weight is null, non-positive, or over 40 kg | Quarantine |
| `DQ_SHP_004_UNKNOWN_ROUTE` | Shipment | Route not in reference table | Quarantine |
| `DQ_SHP_005_UNKNOWN_HUB` | Shipment | Origin/destination hub missing | Quarantine |
| `DQ_SHP_006_UNKNOWN_SERVICE` | Shipment | Service level missing | Quarantine |
| `DQ_SHP_007_ROUTE_HUB_MISMATCH` | Shipment | Route endpoints disagree with shipment | Quarantine |
| `DQ_SHP_008_INVALID_PROMISE` | Shipment | Promise is not after creation | Quarantine |
| `DQ_EVT_001_DUPLICATE_ID` | Event | Repeated event ID | Keep first, quarantine later rows |
| `DQ_EVT_002_REQUIRED_FIELD` | Event | Required value is null | Quarantine |
| `DQ_EVT_003_UNKNOWN_SHIPMENT` | Event | Parent ID absent from bronze shipments | Quarantine |
| `DQ_EVT_004_INVALID_PARENT_SHIPMENT` | Event | Parent failed shipment rules | Quarantine |
| `DQ_EVT_005_UNKNOWN_HUB` | Event | Hub not in reference table | Quarantine |
| `DQ_EVT_006_INVALID_SEQUENCE` | Event | Type does not match lifecycle position | Quarantine |
| `DQ_EVT_007_BEFORE_SHIPMENT` | Event | Event precedes shipment creation | Quarantine |

Late arrival is not automatically a quality failure. A valid record with
`arrival_lag_days >= 2` is retained and surfaced as an operational observability metric.

Physical column names and selected physical types are validated before bronze ingestion. Each
file hash must also match the generation manifest.

