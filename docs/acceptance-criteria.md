# Validation checklist

I use this checklist to separate implemented work from work that has been tested in its target
environment.

## Local pipeline

- [x] A fixed seed generates exactly 250,000 unique base shipments.
- [x] Event data is split into at least five daily arrival batches.
- [x] Repeating generation with the same parameters produces identical file hashes.
- [x] The defect manifest contains expected counts for every injected defect type.
- [x] Bronze records the source path, hash, run ID, row number, and ingestion time.
- [x] Silver enforces schemas, deduplicates, validates references, and quarantines failures.
- [x] A repeated run preserves business-row counts and skips unchanged inputs.
- [x] Replacing one source batch reprocesses that path without duplicating rows.
- [x] Valid late-arriving events are retained and identified by arrival lag.
- [x] Gold contains shipment/event facts and date, hub, route, service, and quality tables.
- [x] Python and SQL tests pass locally.
- [x] The full run writes a machine-readable summary under `data/evidence/`.
- [x] Architecture, table grain, quality rules, KPIs, and setup are documented.
- [x] GitHub Actions passes on the remote repository.

## Microsoft Fabric

- [ ] Lakehouse items are created in an authorised Fabric workspace.
- [ ] Bronze, silver, and gold are written as Delta tables through Fabric notebooks.
- [ ] A parameterised Data Factory pipeline runs ingestion and transformations.
- [ ] A repeated pipeline run is captured as idempotency evidence.
- [ ] Late and replacement batches are tested in Fabric.
- [ ] SQL analytics endpoint checks pass against the gold tables.
- [ ] Fabric item definitions are synchronised to Git where supported.

## Power BI

- [x] The local semantic model refreshes successfully in Power BI Desktop.
- [x] Relationships, measures, formats, and hidden technical fields are checked.
- [x] Executive Overview, Hub Route Performance, and Data Quality Exceptions are reviewed.
- [x] Headline visual totals reconcile to the gold SQL results.
- [x] Layout, slicer controls, readable contrast, alt text, and keyboard order are reviewed.
- [x] Screenshots are saved without workspace, tenant, or personal information.
