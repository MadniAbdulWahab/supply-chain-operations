# Draft Fabric notebook: dimensional gold model
# This first block becomes the parameter cell after import.
run_id = "local_test"
rebuild_scope = "all_current_silver"

# Port the declared grains and calculations from:
#   sql/transform/rebuild_gold.sql
# Required Delta outputs:
#   dim_date, dim_hub, dim_route, dim_service_level,
#   fact_shipments, fact_events, fact_data_quality
# After writing, run the Fabric SQL endpoint version of sql/tests/assertions.sql and reconcile
# every total used by the Power BI semantic model.

raise NotImplementedError(
    "Gold Delta transformations are pending validation in a Fabric workspace"
)
