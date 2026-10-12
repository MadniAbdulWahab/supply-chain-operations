# Fabric run book

This is the procedure for running the pipeline in a Microsoft Fabric trial workspace and
collecting evidence that it reproduces the local DuckDB results.

## 1. Prepare the files locally

```powershell
.venv\Scripts\python -m supply_chain_ops generate
.venv\Scripts\python -m supply_chain_ops run
.venv\Scripts\python -m supply_chain_ops reference
.venv\Scripts\python -m supply_chain_ops fabric-notebooks
```

`reference` writes `data/evidence/local_reference.json`, which contains 40 checks (row
counts, quality-rule counts, and KPI totals) with the values from the local run. The last
command refreshes the importable notebooks in `fabric/notebooks/import/`.

Optional local rehearsal with Docker: `.\scripts\test_fabric_notebooks.ps1` runs the same
notebook sources twice with Spark 3.5 and Delta Lake 3.2.

## 2. Workspace and Lakehouse

1. Open <https://app.fabric.microsoft.com/> and start the Fabric trial if it is not active.
2. Create the workspace `ws-supply-chain-ops` with the **Trial** license mode.
3. In the workspace, create a Lakehouse named `lh_supply_chain_ops`. Leave the
   **Lakehouse schemas** option switched off.

## 3. Upload the source files

In the Lakehouse explorer:

1. Under **Files**, create a folder named `bronze` and open it.
2. **Upload → Upload folder**, then choose `data\bronze\source`. The result should be
   `Files/bronze/source/...` with 39 Parquet files.
3. **Upload → Upload files**, then choose `data\bronze\manifest.json`.
4. Under **Files**, create a folder named `reference` and upload
   `data\evidence\local_reference.json` into it.

## 4. Import and attach the notebooks

1. In the workspace, select **Import → Notebook → From this computer** and choose the four
   files in `fabric\notebooks\import\`.
2. Open each notebook. In the explorer pane, add the existing Lakehouse
   `lh_supply_chain_ops` and make sure it is the default Lakehouse.
3. Check that the first code cell is marked as the **Parameters** cell. If it is not, use the
   cell's `...` menu and select **Toggle parameter cell**.

## 5. Run the notebooks

Run the notebooks in order with **Run all**. The first Spark session can take a few minutes
to start.

| Notebook | Expected output |
|---|---|
| `01_ingest_bronze` | `discovered: 39`, `loaded: 39`, `skipped: 0` |
| `02_build_silver` | `silver_shipments 249950`, `silver_events 1249630`, `quarantine_records 520` |
| `03_build_gold` | `fact_shipments 249950`, `fact_events 1249630`, `dim_date 35` |
| `04_validate` | `status: SUCCESS`, assertions 7 of 7, reconciliation 40 of 40 |

If a notebook fails, copy the complete error message from the failed cell.

## 6. Second run (idempotency)

Run `01_ingest_bronze` to `04_validate` again. Notebook 01 should report `loaded: 0` and
`skipped: 39`, and notebook 04 should report SUCCESS again.

## 7. Data pipeline (optional, recommended)

1. Create a **Data pipeline** named `pl_supply_chain_daily`.
2. Add four **Notebook** activities for notebooks 01 to 04 and connect them with
   **On success**.
3. In each activity, add the base parameter `run_id` (String) with the dynamic value
   `@pipeline().RunId`.
4. Run the pipeline and wait for all four activities to succeed.

## 8. Evidence to collect

- the Lakehouse **Tables** list showing the bronze, silver, gold, and `meta_` tables;
- the output of `04_validate`, including the JSON summary;
- a preview of `meta_reconciliation` filtered to the latest `run_id`;
- a query in the **SQL analytics endpoint**, for example
  `SELECT TOP 10 * FROM fact_shipments`;
- the successful pipeline run, if step 7 was completed.

`04_validate` also writes `Files/evidence/run_<run_id>.json`. Do not commit workspace URLs,
tenant IDs, or account names.
