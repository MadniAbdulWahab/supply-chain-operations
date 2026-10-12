# Microsoft Fabric implementation

This directory holds the Spark version of the pipeline for a Fabric Lakehouse: four PySpark
notebooks that write Delta tables, a pipeline specification, and a run book.

## Verification status

| Environment | Status |
|---|---|
| Local Spark 3.5.3 + Delta Lake 3.2 container | Passed on 12 Oct 2026 with the full dataset (250,000 shipments, 1,250,100 event rows) |
| Fabric workspace | Not yet run; follow [RUNBOOK.md](RUNBOOK.md) |

In the local rehearsal, both runs passed all 7 SQL assertions and all 40 reconciliation checks
against the DuckDB results. The second run loaded no files and skipped all 39 unchanged inputs.
The container mounts the source files at the same `/lakehouse/default/Files` path that Fabric
uses, but it is not Fabric: OneLake, the SQL analytics endpoint, and the pipeline are only
verified by a workspace run.

## Fabric items

1. A development Lakehouse named `lh_supply_chain_ops`.
2. Raw source files under `Files/bronze/source/` in OneLake.
3. Managed Delta tables for bronze, silver, quarantine, gold, and run metadata.
4. Parameterised notebooks for ingestion, quality processing, dimensional modelling, and
   validation ([notebooks/README.md](notebooks/README.md)).
5. A Data Factory pipeline that passes the pipeline run ID to each notebook.
6. SQL analytics endpoint checks and a Power BI semantic model.

## Workspace setup

The run requires an account with access to a Fabric capacity or trial. The steps are in
[RUNBOOK.md](RUNBOOK.md). Workspace URLs, tenant IDs, connection strings, tokens, and other
account details do not belong in this repository.

## Design notes

- A Lakehouse stores files and managed Delta tables in OneLake and automatically provides a
  SQL analytics endpoint.
- Spark notebooks handle table writes; the SQL analytics endpoint is used for querying and
  validation.
- Notebook activities pass simple run parameters from the Data Factory pipeline.
- Fabric Git integration stores item metadata, not the generated table data.
- Delta change data feed may be useful for later downstream incremental processing, but it is
  not part of the current implementation.

Microsoft documentation used for this design:

- <https://learn.microsoft.com/en-us/fabric/data-engineering/tutorial-lakehouse-introduction>
- <https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint>
- <https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity>
- <https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-git-deployment-pipelines>
- <https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-change-data-feed>
