# Fabric verification

Last updated: 2026-10-12 (Europe/Berlin).

The notebooks in `fabric/notebooks/` were imported into a Microsoft Fabric trial workspace,
attached to the Lakehouse `lh_supply_chain_ops`, and run against the full dataset uploaded to
OneLake (39 Parquet files: 250,000 shipments and 1,250,100 event rows).

## Results

| Run | How it was started | Files loaded / skipped | SQL assertions | Reconciliation with DuckDB | Status |
|---|---|---:|---:|---:|---|
| 1 | Notebooks run manually in order | 39 / 0 | 7 / 7 | 40 / 40 | SUCCESS |
| 2 | Notebooks run manually in order | 0 / 39 | 7 / 7 | 40 / 40 | SUCCESS |
| 3 | Data pipeline `pl_supply_chain_daily` | 0 / 39 | passed | passed | Succeeded |

The 40 reconciliation checks compare Fabric with the local DuckDB run: row counts for all 12
tables, the counts for all 15 quality rules, and 13 KPI totals such as delivered and on-time
shipments, total delay minutes, transit hours, late-arriving events, and key checksums. The
pipeline's `Validate` activity raises an error when any check fails, so its success confirms
the checks for run 3.

The pipeline ran the four notebook activities in sequence with `@pipeline().RunId` as the
`run_id` parameter. Activity durations were 1 min 23 s (bronze), 1 min 51 s (silver),
3 min 55 s (gold), and 1 min 53 s (validation).

The gold tables were also queried with T-SQL through the Lakehouse SQL analytics endpoint
(`SELECT TOP 10 * FROM fact_shipments`).

## Runtime

Fabric reported Spark version `4.1.1.5.5.20260930`. Before the workspace run, the same
notebook sources passed the same checks locally with Spark 3.5.3 and Delta Lake 3.2
(`scripts/test_fabric_notebooks.ps1`), so they ran unchanged on both Spark versions.

## Evidence

- [Run 1 validation output](images/fabric-validate-run1.png)
- [Run 2 validation output](images/fabric-validate-run2.png)
- [Pipeline run](images/fabric-pipeline-run.png)
- [SQL analytics endpoint query](images/fabric-sql-endpoint.png)

## Not covered

- The Power BI report was built and reconciled locally in Power BI Desktop. A Direct Lake
  semantic model on the Fabric Lakehouse has not been built.
- The pipeline was run on demand; no schedule or alerting was configured.
