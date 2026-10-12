# Fabric notebook: bronze ingestion
# Tested local equivalent: src/supply_chain_ops/pipeline.py

# %% tags=["parameters"]
run_id = ""
source_root = "Files/bronze"

# %%
import glob
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone

import pyarrow.parquet as pq
from pyspark.sql import Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
run_id = run_id or str(uuid.uuid4())
local_root = os.path.join("/lakehouse/default", source_root)

SOURCE_COLUMNS = {
    "shipments": [
        ("shipment_id", "STRING"),
        ("created_ts", "TIMESTAMP"),
        ("promised_delivery_ts", "TIMESTAMP"),
        ("origin_hub_id", "STRING"),
        ("destination_hub_id", "STRING"),
        ("route_id", "STRING"),
        ("service_level_id", "STRING"),
        ("customer_region", "STRING"),
        ("weight_kg", "DOUBLE"),
    ],
    "events": [
        ("event_id", "STRING"),
        ("shipment_id", "STRING"),
        ("event_sequence", "SMALLINT"),
        ("event_type", "STRING"),
        ("event_ts", "TIMESTAMP"),
        ("hub_id", "STRING"),
    ],
    "hubs": [
        ("hub_id", "STRING"),
        ("hub_name", "STRING"),
        ("city", "STRING"),
        ("state_code", "STRING"),
    ],
    "routes": [
        ("route_id", "STRING"),
        ("origin_hub_id", "STRING"),
        ("destination_hub_id", "STRING"),
        ("distance_km", "INT"),
    ],
    "service_levels": [
        ("service_level_id", "STRING"),
        ("service_level_name", "STRING"),
        ("sla_days", "INT"),
        ("base_price_eur", "DOUBLE"),
    ],
}
LINEAGE_COLUMNS = [
    ("source_file", "STRING NOT NULL"),
    ("source_sha256", "STRING NOT NULL"),
    ("source_row", "BIGINT NOT NULL"),
    ("ingestion_run_id", "STRING NOT NULL"),
    ("ingested_at", "TIMESTAMP NOT NULL"),
]
TARGET_TABLES = {
    "shipments": "bronze_shipments",
    "events": "bronze_events",
    "hubs": "bronze_hubs",
    "routes": "bronze_routes",
    "service_levels": "bronze_service_levels",
}
TABLE_FILE_KINDS = {
    "source/shipments/shipments.parquet": "shipments",
    "source/reference/hubs.parquet": "hubs",
    "source/reference/routes.parquet": "routes",
    "source/reference/service_levels.parquet": "service_levels",
}
# Physical Parquet types accepted for the columns that later rules depend on.
TYPE_EXPECTATIONS = {
    "shipment_id": ("string",),
    "event_id": ("string",),
    "event_sequence": ("int8", "int16", "int32", "int64"),
    "created_ts": ("timestamp",),
    "promised_delivery_ts": ("timestamp",),
    "event_ts": ("timestamp",),
    "weight_kg": ("double", "float"),
    "distance_km": ("int",),
}
RUN_SCHEMA = (
    "run_id STRING, started_at TIMESTAMP, completed_at TIMESTAMP, status STRING, "
    "stage STRING, discovered_files BIGINT, loaded_files BIGINT, skipped_files BIGINT, "
    "error_message STRING"
)
RUN_FIELDS = [part.split()[0] for part in RUN_SCHEMA.split(", ")]

# %%
spark.sql(f"CREATE TABLE IF NOT EXISTS meta_pipeline_runs ({RUN_SCHEMA}) USING DELTA")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS meta_ingestion_files (
        source_file STRING NOT NULL,
        file_kind STRING NOT NULL,
        sha256 STRING NOT NULL,
        file_size BIGINT NOT NULL,
        loaded_at TIMESTAMP NOT NULL,
        run_id STRING NOT NULL,
        status STRING NOT NULL
    ) USING DELTA
    """
)
for kind, table in TARGET_TABLES.items():
    column_ddl = ", ".join(f"{name} {dtype}" for name, dtype in SOURCE_COLUMNS[kind])
    lineage_ddl = ", ".join(f"{name} {dtype}" for name, dtype in LINEAGE_COLUMNS)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({column_ddl}, {lineage_ddl}) USING DELTA")


def save_run(record):
    row = tuple(record[field] for field in RUN_FIELDS)
    spark.createDataFrame([row], RUN_SCHEMA).createOrReplaceTempView("run_update")
    spark.sql(
        """
        MERGE INTO meta_pipeline_runs AS t USING run_update AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
        """
    )


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_kind(relative_path):
    if relative_path in TABLE_FILE_KINDS:
        return TABLE_FILE_KINDS[relative_path]
    if relative_path.startswith("source/shipment_events/batch_date="):
        return "events"
    raise ValueError(f"unexpected source file: {relative_path}")


def validate_schema(path, kind):
    schema = pq.read_schema(path)
    expected = [name for name, _ in SOURCE_COLUMNS[kind]]
    if schema.names != expected:
        raise ValueError(
            f"schema mismatch for {path}: expected {expected}, received {schema.names}"
        )
    for field in schema:
        prefixes = TYPE_EXPECTATIONS.get(field.name)
        if prefixes and not str(field.type).startswith(prefixes):
            raise ValueError(f"type mismatch for {path}:{field.name}: received {field.type}")


def read_source(relative_path, kind, file_hash):
    # One source file is read in file order, which matches DuckDB's row_number() OVER ().
    file_order = Window.orderBy(F.monotonically_increasing_id())
    return spark.read.parquet(f"{source_root}/{relative_path}").select(
        *[F.col(name).cast(dtype).alias(name) for name, dtype in SOURCE_COLUMNS[kind]],
        F.lit(relative_path).alias("source_file"),
        F.lit(file_hash).alias("source_sha256"),
        F.row_number().over(file_order).cast("bigint").alias("source_row"),
        F.lit(run_id).alias("ingestion_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


# %%
run = dict.fromkeys(RUN_FIELDS)
run.update(
    run_id=run_id, started_at=datetime.now(timezone.utc), status="RUNNING", stage="bronze"
)
save_run(run)

try:
    with open(os.path.join(local_root, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    expected_hashes = {item["path"]: item["sha256"] for item in manifest["files"]}
    source_files = sorted(
        os.path.relpath(path, local_root).replace(os.sep, "/")
        for path in glob.glob(os.path.join(local_root, "source", "**", "*.parquet"), recursive=True)
    )
    run["discovered_files"] = len(source_files)
    if set(source_files) != set(expected_hashes):
        raise ValueError("source file set differs from manifest")

    already_loaded = {
        row.source_file: (row.sha256, row.status)
        for row in spark.table("meta_ingestion_files").collect()
    }
    pending = {kind: [] for kind in TARGET_TABLES}
    skipped = 0
    for relative in source_files:
        absolute = os.path.join(local_root, relative)
        kind = file_kind(relative)
        file_hash = sha256(absolute)
        if file_hash != expected_hashes[relative]:
            raise ValueError(f"hash mismatch for {relative}")
        validate_schema(absolute, kind)
        if already_loaded.get(relative) == (file_hash, "SUCCESS"):
            skipped += 1
            continue
        pending[kind].append((relative, file_hash, os.path.getsize(absolute)))

    loaded = 0
    for kind, files in pending.items():
        if not files:
            continue
        frames = [read_source(relative, kind, file_hash) for relative, file_hash, _ in files]
        batch = frames[0]
        for frame in frames[1:]:
            batch = batch.unionByName(frame)
        # replaceWhere swaps only the rows of these source files in one Delta commit, so a
        # changed file is replaced and a rerun after a failure cannot duplicate rows.
        file_list = ", ".join(sql_string(relative) for relative, _, _ in files)
        (
            batch.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"source_file IN ({file_list})")
            .saveAsTable(TARGET_TABLES[kind])
        )
        loaded_at = datetime.now(timezone.utc)
        spark.createDataFrame(
            [
                (relative, kind, file_hash, size, loaded_at, run_id, "SUCCESS")
                for relative, file_hash, size in files
            ],
            "source_file STRING, file_kind STRING, sha256 STRING, file_size BIGINT, "
            "loaded_at TIMESTAMP, run_id STRING, status STRING",
        ).createOrReplaceTempView("loaded_files")
        spark.sql(
            """
            MERGE INTO meta_ingestion_files AS t USING loaded_files AS s
            ON t.source_file = s.source_file
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
            """
        )
        loaded += len(files)

    run.update(loaded_files=loaded, skipped_files=skipped)
    save_run(run)
except Exception as exc:
    run.update(
        status="FAILED", completed_at=datetime.now(timezone.utc), error_message=str(exc)[:2000]
    )
    save_run(run)
    raise

print(json.dumps({"run_id": run_id, "discovered": len(source_files), "loaded": loaded,
                  "skipped": skipped}))
