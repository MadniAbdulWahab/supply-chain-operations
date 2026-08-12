# Local verification

Last updated: 2026-08-12 (Europe/Berlin).

## Full dataset run

The default dataset was executed locally with Python 3.11, DuckDB 1.5.5, and Parquet.

| Check | Result |
|---|---:|
| Synthetic shipments | 250,000 |
| Source event rows | 1,250,100 |
| Source files discovered | 39 |
| First ingestion | 39 loaded, 0 skipped |
| Identical second ingestion | 0 loaded, 39 skipped |
| Valid silver shipments | 249,950 |
| Valid silver events | 1,249,630 |
| Quarantined rows | 520 |
| Gold shipment fact rows | 249,950 |
| Gold event fact rows | 1,249,630 |

The quarantine total consists of the injected invalid or duplicate source rows plus events
whose parent shipments were rejected. Valid events arriving two days late remain in silver
and gold and are reported separately.

The reproducibility check generated the dataset a second time with the same parameters. Both
runs produced this manifest fingerprint:

`8851831e6d37c7f5f734fbeabafc2ccb3801419d43abfcbb53b12a4212196dd7`

Local run details are written to `data/evidence/latest_run.json`. That file is excluded from
Git because it contains run-specific timestamps and paths.

## Automated tests

`pytest -q` completed with 6 passing tests. The suite covers deterministic generation,
manifest volumes, guarded replacement, quality rules, idempotent reruns, hash validation, and
same-path batch reprocessing. End-to-end pipeline tests also execute
`sql/tests/assertions.sql`.

`ruff check src tests` and `ruff format --check src tests` also passed.

## Checks outside the local pipeline

- GitHub Actions has not run because the repository has not been pushed yet.
- Fabric items have not been created or executed.
- The Power BI source project has been generated, but Desktop refresh and visual review are
  still pending.
