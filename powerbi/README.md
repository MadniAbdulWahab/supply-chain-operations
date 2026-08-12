# Power BI report

This directory contains the report layout, semantic model, DAX measures, and the script I use
to rebuild the Power BI Project files.

The report reads only from the gold Parquet tables and contains three pages: Executive
Overview, Hub Route Performance, and Data Quality Exceptions. The intended relationships and
table grains are documented in `model-contract.json`; the measure definitions are also kept
in `measures.dax` for easier review.

## Local Desktop build

After running the local pipeline, build a Desktop-specific copy with absolute data paths:

```powershell
$goldDataRoot = (Resolve-Path data/gold).Path
node scripts/build_powerbi_project.mjs --output powerbi/.runtime `
  --project-name SupplyChainOperationsLocal --data-root $goldDataRoot
```

Open `powerbi/.runtime/SupplyChainOperationsLocal.pbip`. The `.runtime` directory is ignored
because it contains machine-specific paths.

The project under `powerbi/project/` is the portable source version. Its `pDataRoot` parameter
must be replaced or regenerated before it can load local Parquet files.

Binary `.pbix` and `.pbit` files are excluded because they can contain cached data and local
connection details.

## Status

The PBIP/PBIR structure passes local report-author validation. A successful Desktop refresh,
measure reconciliation, and final visual review are still outstanding.
