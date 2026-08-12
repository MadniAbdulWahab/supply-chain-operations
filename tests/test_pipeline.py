from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from supply_chain_ops.config import ProjectPaths
from supply_chain_ops.generator import generate_dataset
from supply_chain_ops.pipeline import run_pipeline


def test_pipeline_quality_rules_and_idempotency(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path / "data")
    generate_dataset(paths.bronze, shipment_count=1_000, seed=11)
    manifest = json.loads((paths.bronze / "manifest.json").read_text(encoding="utf-8"))

    first = run_pipeline(paths)
    second = run_pipeline(paths)

    assert first["status"] == "SUCCESS"
    assert first["loaded_files"] == first["discovered_files"]
    assert second["loaded_files"] == 0
    assert second["skipped_files"] == second["discovered_files"]
    assert first["table_counts"] == second["table_counts"]

    expected = manifest["expected_counts"]
    quality = second["quality_counts"]
    assert quality["DQ_SHP_003_WEIGHT_RANGE"] == expected["shipment_negative_weight"]
    assert quality["DQ_SHP_004_UNKNOWN_ROUTE"] == expected["shipment_unknown_route"]
    assert quality["DQ_EVT_001_DUPLICATE_ID"] == expected["event_duplicate"]
    assert quality["DQ_EVT_002_REQUIRED_FIELD"] == expected["event_null_timestamp"]
    assert quality["DQ_EVT_003_UNKNOWN_SHIPMENT"] == expected["event_unknown_shipment"]
    assert quality["DQ_EVT_005_UNKNOWN_HUB"] == expected["event_unknown_hub"]
    assert quality["DQ_EVT_006_INVALID_SEQUENCE"] == expected["event_invalid_sequence"]

    with duckdb.connect(str(paths.database), read_only=True) as connection:
        late_count = connection.execute(
            "SELECT count(*) FROM silver_events WHERE arrival_lag_days >= 2"
        ).fetchone()[0]
        assert late_count == expected["late_arriving_valid_event"]


def test_pipeline_refuses_tampered_source(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path / "data")
    generate_dataset(paths.bronze, shipment_count=100, seed=3)
    shipment_file = paths.bronze / "source" / "shipments" / "shipments.parquet"
    with shipment_file.open("ab") as handle:
        handle.write(b"tampered")

    try:
        run_pipeline(paths)
    except ValueError as exc:
        assert "hash mismatch" in str(exc)
    else:
        raise AssertionError("expected manifest integrity check to fail")


def test_changed_batch_is_replaced_without_duplication(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path / "data")
    generate_dataset(paths.bronze, shipment_count=500, seed=13)
    first = run_pipeline(paths)
    batch_path = sorted((paths.bronze / "source" / "shipment_events").rglob("*.parquet"))[0]
    table = pq.ParquetFile(batch_path).read()
    pq.write_table(
        table.take(list(reversed(range(table.num_rows)))), batch_path, compression="zstd"
    )

    digest = hashlib.sha256(batch_path.read_bytes()).hexdigest()
    relative = batch_path.relative_to(paths.bronze).as_posix()
    manifest_path = paths.bronze / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in manifest["files"]:
        if item["path"] == relative:
            item["sha256"] = digest
            item["bytes"] = batch_path.stat().st_size
            break
    else:
        raise AssertionError("batch not found in manifest")
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    second = run_pipeline(paths)

    assert second["loaded_files"] == 1
    assert second["skipped_files"] == second["discovered_files"] - 1
    assert second["table_counts"] == first["table_counts"]
