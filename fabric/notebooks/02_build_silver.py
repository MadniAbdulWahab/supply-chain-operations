# Draft Fabric notebook: silver quality processing
# This first block becomes the parameter cell after import.
run_id = "local_test"
rebuild_scope = "all_current_bronze"

# Port the first-failure-precedence rules from:
#   sql/transform/rebuild_silver.sql
# Required outputs as managed Delta tables:
#   silver_shipments, silver_events, quarantine_records
# Required behavioral checks:
#   - duplicate event and shipment IDs are deterministic;
#   - rejected rows carry rule, raw record, source path/hash/row, run ID, and timestamp;
#   - valid late-arriving events are retained with arrival_lag_days;
#   - a repeat run preserves business counts.

raise NotImplementedError(
    "Silver Delta transformations are pending validation in a Fabric workspace"
)
