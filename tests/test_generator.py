from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from supply_chain_ops.generator import generate_dataset, manifest_fingerprint


def test_generation_is_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first" / "bronze"
    second = tmp_path / "second" / "bronze"
    generate_dataset(first, shipment_count=500, seed=42)
    generate_dataset(second, shipment_count=500, seed=42)

    assert manifest_fingerprint(first / "manifest.json") == manifest_fingerprint(
        second / "manifest.json"
    )


def test_generator_creates_declared_volume_batches_and_defects(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    result = generate_dataset(bronze, shipment_count=1_000, seed=7)
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    shipment_table = pq.read_table(bronze / "source" / "shipments" / "shipments.parquet")
    event_files = list((bronze / "source" / "shipment_events").rglob("*.parquet"))

    assert shipment_table.num_rows == 1_000
    assert len(event_files) >= 5
    assert result.event_row_count == 5_000 + manifest["expected_counts"]["event_duplicate"]
    assert all(count > 0 for count in manifest["expected_counts"].values())


def test_generation_requires_explicit_force_to_replace(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    generate_dataset(bronze, shipment_count=100, seed=1)

    try:
        generate_dataset(bronze, shipment_count=100, seed=1)
    except FileExistsError as exc:
        assert "force=True" in str(exc)
    else:
        raise AssertionError("expected replacement guard to raise")
