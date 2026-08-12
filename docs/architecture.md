# Architecture

## Local-first implementation

```text
Deterministic Python generator
    |  Parquet + defect manifest, partitioned by arrival batch date
    v
Bronze (immutable source-shaped rows + ingestion metadata)
    |  schema / required / uniqueness / reference / sequence rules
    +-------------------------------> Quarantine + DQ results
    v
Silver (typed, deduplicated, conformed shipments and events)
    |  SQL dimensional transformations
    v
Gold (dimensions + shipment/event facts + DQ fact)
    |
    +--> SQL assertions
    +--> Power BI semantic-model contract
```

DuckDB is the local execution engine and Parquet is the local storage format. This proves the
logic and invariants without claiming Delta transaction semantics or Fabric service behavior.

## Intended Fabric mapping (unverified)

| Local component | Fabric target | Verification state |
|---|---|---|
| `data/bronze` Parquet | OneLake `Files/bronze` plus managed bronze Delta tables | Unverified |
| DuckDB transformations | Parameterized Fabric PySpark notebooks | Unverified |
| Local orchestrator | Fabric Data Factory notebook activities | Unverified |
| Silver/gold Parquet | Managed Delta tables under Lakehouse `Tables` | Unverified |
| DuckDB SQL checks | Lakehouse SQL analytics endpoint T-SQL checks | Unverified |
| BI contract | Power BI semantic model, preferably Direct Lake where appropriate | Unverified |

Fabric writes belong in Spark/notebooks because the Lakehouse SQL analytics endpoint is a
read-only query surface over Delta tables. The endpoint is suitable for validation queries,
views, security objects, and Power BI connectivity, not INSERT/UPDATE/DELETE into Lakehouse
tables. Pipeline notebook activities will pass simple batch/run parameters through a notebook
parameter cell.

## Idempotency boundary

The local control table identifies each source path and SHA-256 hash. An unchanged successful
file is skipped. A changed file at a known path is replaced transactionally in bronze, and
downstream layers are rebuilt deterministically from the complete bronze snapshot. This is
incremental at ingestion and safe for reprocessing; the initial local version deliberately
chooses a full downstream rebuild for clarity. The Fabric version will preserve the same
contract and may replace the rebuild with Delta `MERGE`/change-data-feed only after testing.

