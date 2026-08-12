# Fabric notebook drafts

These files define the parameters and outputs planned for the Fabric implementation. They are
kept separate from the tested local pipeline because no Fabric workspace run has been
completed yet.

| Notebook | Responsibility | Tested local equivalent |
|---|---|---|
| `01_ingest_bronze.py` | Validate source schema and load file-scoped bronze rows | `pipeline.py` |
| `02_build_silver.py` | Deduplicate, validate, quarantine, and flag late arrivals | `rebuild_silver.sql` |
| `03_build_gold.py` | Build dimensions, facts, and quality results | `rebuild_gold.sql` |

In Fabric, the first block in each notebook will be marked as the parameter cell and the
notebooks will be attached to the project Lakehouse. Environment-specific values will be
supplied by the pipeline rather than stored in Git.
