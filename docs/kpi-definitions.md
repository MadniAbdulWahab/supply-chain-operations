# KPI definitions

All delivery KPIs use valid silver/gold rows. Data-quality exclusions are visible on the Data
Quality page and must not be hidden from viewers.

| KPI | Definition | Decision supported |
|---|---|---|
| Shipments | Distinct valid `shipment_id` | Network workload |
| Delivered shipments | Shipments with `actual_delivery_ts` | Completed workload |
| On-time delivery rate | Delivered on/before promise divided by delivered shipments | Promise reliability |
| Average delay hours | Mean positive `delay_minutes / 60` for late deliveries | Severity of misses |
| P90 transit hours | 90th percentile of delivered transit duration | Tail performance |
| Exception shipments | Valid shipments whose latest event is not DELIVERED | Operational follow-up |
| Late-arriving event rate | Valid events arriving at least two days late / valid events | Source latency |
| Quarantine rate | Quarantined source rows / total bronze source rows | Data trust |
| First-pass quality rate | 1 minus quarantine rate | Platform reliability |

Filters: created date, promised date, service level, origin hub, destination hub, route, and
customer region. The report must make the active date context explicit because shipment facts
contain multiple business dates.

