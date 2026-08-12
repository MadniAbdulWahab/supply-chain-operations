# Draft Fabric notebook: bronze ingestion
# This first block becomes the parameter cell after import.
run_id = "local_test"
source_relative_path = "Files/bronze/source/shipment_events/batch_date=2026-01-01/events.parquet"
source_kind = "events"
source_sha256 = ""
allow_path_reprocess = False

# Planned ingestion behaviour:
#   1. compare the selected schema with the expected schema below;
#   2. verify the orchestrator-provided file hash against a hash manifest;
#   3. skip a successful path+hash pair;
#   4. for a changed known path, replace only rows carrying that source path inside one Delta
#      transaction/merge strategy validated in Fabric;
#   5. write run/file lineage columns on every bronze record.

EXPECTED_COLUMNS = {
    "shipments": [
        "shipment_id", "created_ts", "promised_delivery_ts", "origin_hub_id",
        "destination_hub_id", "route_id", "service_level_id", "customer_region", "weight_kg",
    ],
    "events": [
        "event_id", "shipment_id", "event_sequence", "event_type", "event_ts", "hub_id",
    ],
}

if source_kind not in EXPECTED_COLUMNS:
    raise ValueError(f"Unsupported source_kind: {source_kind}")

source_df = spark.read.parquet(source_relative_path)
if source_df.columns != EXPECTED_COLUMNS[source_kind]:
    raise ValueError(
        f"Schema mismatch: expected {EXPECTED_COLUMNS[source_kind]}, got {source_df.columns}"
    )

raise NotImplementedError(
    "Delta write logic is pending validation in a Fabric workspace"
)
