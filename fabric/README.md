# Microsoft Fabric design

The working implementation currently runs with DuckDB and Parquet. This directory maps that
design to Microsoft Fabric and holds draft notebook sources for the cloud version. I have not
executed these files in a Fabric workspace yet.

## Planned Fabric items

1. A development Lakehouse named `lh_supply_chain_ops`.
2. Raw source files under `Files/bronze/source/` in OneLake.
3. Managed Delta tables for bronze, silver, quarantine, and gold entities.
4. Parameterised notebooks for ingestion, quality processing, and dimensional modelling.
5. A Data Factory pipeline that supplies run, source-path, and reprocessing parameters.
6. SQL analytics endpoint checks and a Power BI semantic model.

## Workspace setup

The next Fabric step requires an account with access to a Fabric capacity or trial. After
signing in at <https://app.fabric.microsoft.com/>, I need to create or select a development
workspace and attach a Lakehouse to the notebooks. Workspace URLs, tenant IDs, connection
strings, tokens, and other account details do not belong in this repository.

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
