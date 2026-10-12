# Report screenshots

This directory contains the final Power BI report pages captured on 2026-08-13 after local
refresh, KPI reconciliation, and visual review:

- `executive-overview.png`
- `hub-route-performance.png`
- `data-quality-exceptions.png`

The images contain only the report canvas. Power BI account, tenant, workspace, application
chrome, and connection details were excluded.

## Fabric run evidence

Captured on 2026-10-12 in the Fabric trial workspace (see
[fabric-verification.md](../fabric-verification.md)):

- `fabric-validate-run1.png`: first notebook run, 7/7 assertions and 40/40 reconciliation checks
- `fabric-validate-run2.png`: repeat run, 0 files loaded and 39 skipped, checks still passing
- `fabric-pipeline-run.png`: Data Factory pipeline with four succeeded notebook activities
- `fabric-sql-endpoint.png`: `fact_shipments` queried through the SQL analytics endpoint

These images show workspace, Lakehouse, and run identifiers, which are not secrets. They do not
show URLs, tenant IDs, or connection details.
