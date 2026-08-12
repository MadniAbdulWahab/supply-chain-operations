# Power BI report

This directory contains the report layout, semantic model, DAX measures, and the script I use
to rebuild the Power BI Project files.

The report reads only from the gold Parquet tables and contains three pages: Executive
Overview, Hub Route Performance, and Data Quality Exceptions. The intended relationships and
table grains are documented in `model-contract.json`; the measure definitions are also kept
in `measures.dax` for easier review.

## Local Desktop build

After running the local pipeline, use the launcher from the repository root:

```powershell
.\scripts\open_powerbi.ps1
```

The script rebuilds and opens `powerbi/.runtime/SupplyChainOperationsLocal.pbip`. The
`.runtime` directory is ignored because it contains machine-specific paths.

The project under `powerbi/project/` is the portable source version. It returns typed empty
tables while `pDataRoot` has its default `__DATA_ROOT__` value, allowing reviewers to inspect
the report and semantic-model source without receiving invalid-path errors. It is not the file
used for local data refresh.

Binary `.pbix` and `.pbit` files are excluded because they can contain cached data and local
connection details.

## Status

The PBIP/PBIR structure passes local report-author validation. The local model refreshed in
Power BI Desktop and the headline measures were reconciled to DuckDB. All three pages passed
the desktop layout review, and privacy-safe report-canvas screenshots are stored in
`docs/images/`.
