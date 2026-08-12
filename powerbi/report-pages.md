# Report page contract

## 1. Executive Overview

Audience: operations leadership. Answer: Are promises being met, where is risk growing, and
what needs attention now?

- KPI cards: Shipments, Delivered Shipments, On-Time Delivery %, Average Delay Hours, P90
  Transit Hours.
- Weekly on-time trend with explicit create-date context.
- Shipment volume and on-time rate by service level.
- Top five routes by late shipment count, with a minimum-volume guard.
- Slicers: date, service, customer region; a visible reset-filters control.

## 2. Hub / Route Performance

Audience: network managers. Answer: Which parts of the network drive misses?

- Route matrix: origin, destination, shipments, on-time %, delay hours, P90 transit.
- Hub comparison with volume and late-arriving event rate.
- Trend for the selected route/hub.
- Drill-through shipment detail using synthetic IDs only.
- Tooltip states denominator and active date role.

## 3. Data Quality / Exceptions

Audience: data platform and operations analysts. Answer: Can the report be trusted, and where
should investigation start?

- Quarantined Records, affected source files, and late-arriving event rate cards.
- Failed rows by rule and entity.
- Run-to-run trend by evaluation time.
- Source-file/rule matrix for triage.
- Redacted synthetic record detail; no raw JSON in executive views.

## Visual review checklist

- Use consistent colors: neutral blue for volume, green for on-time, amber for latency, red for
  failed quality rules. Color is never the only signal.
- All visuals have concise titles, alt text, keyboard focus order, and readable contrast.
- Empty selections display explanatory text rather than a misleading zero.
- Cross-highlighting and drill-through are tested on every page.
- Mobile layout is reviewed after desktop totals reconcile to SQL.
- Final screenshots must not include workspace or tenant details.
