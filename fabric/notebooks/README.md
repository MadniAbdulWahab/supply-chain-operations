# Fabric notebooks

The `.py` files are the reviewed notebook sources in percent format (`# %%` starts a cell).
`fabric/notebooks/import/` holds the generated `.ipynb` versions for **Import → Notebook** in
Fabric; rebuild them with `python -m supply_chain_ops fabric-notebooks`. A test fails when
they are out of date.

| Notebook | Responsibility | Tested local equivalent |
|---|---|---|
| `01_ingest_bronze.py` | Validate manifest, hashes, and schemas; load new or changed files into bronze Delta tables | `pipeline.py` |
| `02_build_silver.py` | Deduplicate, validate, quarantine, and flag late arrivals | `rebuild_silver.sql` |
| `03_build_gold.py` | Build dimensions, facts, and quality results | `rebuild_gold.sql` |
| `04_validate.py` | Run the SQL assertions and reconcile 40 checks with the local DuckDB run | `assertions.sql` |

The first code cell in each notebook is the parameter cell. A Data Factory pipeline passes
`run_id`; when it is empty, notebook 01 creates one and later notebooks use the latest run.
Run status is kept in `meta_pipeline_runs`, file lineage in `meta_ingestion_files`, and check
results in `meta_reconciliation`.

## Spark SQL differences from the DuckDB version

- Timestamps are read as UTC `TIMESTAMP` values, which the SQL analytics endpoint and
  Direct Lake support, instead of `TIMESTAMP_NTZ`.
- `date_diff('minute', a, b)` in DuckDB counts minute boundaries, so the Spark version
  compares `floor(unix_timestamp(x) / 60)` values instead of using `timestampdiff`.
- DuckDB numbers weekdays from Sunday = 0 and Spark from Sunday = 1; `dim_date` keeps the
  DuckDB numbering.
- `dim_date` bounds are taken from one union of shipment and event dates, which gives the
  same range as the DuckDB scalar subqueries.

## Local rehearsal

`scripts/test_fabric_notebooks.ps1` runs the unchanged sources in a Docker container with
Python 3.11, Spark 3.5.3, and Delta Lake 3.2. The bronze files are mounted at the same
`/lakehouse/default/Files` path that Fabric uses. The script runs the four notebooks twice
and writes `data/evidence/local_spark_run.json`.
