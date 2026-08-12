from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from supply_chain_ops.config import DEFAULT_SEED, DEFAULT_SHIPMENT_COUNT

HUBS = [
    ("H-BER", "Berlin Nord", "Berlin", "DE-BE"),
    ("H-HAM", "Hamburg Elbe", "Hamburg", "DE-HH"),
    ("H-MUC", "Muenchen Isar", "Muenchen", "DE-BY"),
    ("H-CGN", "Koeln Rhein", "Koeln", "DE-NW"),
    ("H-FRA", "Frankfurt Main", "Frankfurt am Main", "DE-HE"),
    ("H-LEJ", "Leipzig Mitte", "Leipzig", "DE-SN"),
    ("H-STR", "Stuttgart Neckar", "Stuttgart", "DE-BW"),
    ("H-DUS", "Duesseldorf West", "Duesseldorf", "DE-NW"),
    ("H-HAJ", "Hannover Nord", "Hannover", "DE-NI"),
    ("H-NUE", "Nuernberg Sued", "Nuernberg", "DE-BY"),
]

ROUTE_PAIRS = [
    ("H-BER", "H-HAM", 289),
    ("H-BER", "H-LEJ", 190),
    ("H-BER", "H-MUC", 585),
    ("H-HAM", "H-HAJ", 153),
    ("H-HAM", "H-CGN", 424),
    ("H-HAJ", "H-FRA", 350),
    ("H-LEJ", "H-NUE", 302),
    ("H-LEJ", "H-MUC", 430),
    ("H-CGN", "H-FRA", 190),
    ("H-CGN", "H-DUS", 44),
    ("H-FRA", "H-STR", 204),
    ("H-FRA", "H-MUC", 394),
    ("H-STR", "H-MUC", 233),
    ("H-NUE", "H-MUC", 170),
    ("H-DUS", "H-HAM", 401),
    ("H-NUE", "H-FRA", 225),
    ("H-HAJ", "H-BER", 286),
    ("H-MUC", "H-BER", 585),
    ("H-FRA", "H-CGN", 190),
    ("H-HAM", "H-BER", 289),
]

SERVICES = [
    ("S-EXP", "Express", 1, 12.90),
    ("S-STD", "Standard", 2, 7.50),
    ("S-ECO", "Economy", 3, 4.90),
]

EVENT_TYPES = np.array(
    ["CREATED", "PICKED_UP", "DEPARTED_ORIGIN", "ARRIVED_DESTINATION", "DELIVERED"]
)

DEFAULT_DEFECT_TARGETS = {
    "shipment_negative_weight": 20,
    "shipment_unknown_route": 30,
    "event_duplicate": 100,
    "event_null_timestamp": 25,
    "event_unknown_hub": 40,
    "event_unknown_shipment": 20,
    "event_invalid_sequence": 35,
    "late_arriving_valid_event": 250,
}


@dataclass(frozen=True)
class GenerationResult:
    manifest_path: Path
    shipment_count: int
    event_row_count: int
    file_count: int


def _scaled_count(target: int, shipment_count: int, divisor: int = 100) -> int:
    if shipment_count >= DEFAULT_SHIPMENT_COUNT:
        return target
    return min(target, max(1, shipment_count // divisor))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, compression="zstd", use_dictionary=True)


def _reference_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    hubs = pd.DataFrame(HUBS, columns=["hub_id", "hub_name", "city", "state_code"])
    routes = pd.DataFrame(
        [
            (f"R-{index:03d}", origin, destination, distance)
            for index, (origin, destination, distance) in enumerate(ROUTE_PAIRS, 1)
        ],
        columns=["route_id", "origin_hub_id", "destination_hub_id", "distance_km"],
    )
    services = pd.DataFrame(
        SERVICES,
        columns=["service_level_id", "service_level_name", "sla_days", "base_price_eur"],
    )
    return hubs, routes, services


def generate_dataset(
    output_dir: Path,
    shipment_count: int = DEFAULT_SHIPMENT_COUNT,
    seed: int = DEFAULT_SEED,
    force: bool = False,
) -> GenerationResult:
    """Generate a deterministic, source-shaped Parquet dataset and defect manifest."""
    if shipment_count < 100:
        raise ValueError("shipment_count must be at least 100")
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists() and not force:
        raise FileExistsError(f"{manifest_path} already exists; pass force=True to replace it")
    resolved = output_dir.resolve()
    if force and output_dir.exists():
        if output_dir.name.lower() != "bronze" and "pytest" not in str(resolved).lower():
            raise ValueError("refusing to replace a directory that is not named 'bronze'")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    hubs, routes, services = _reference_frames()
    route_index = rng.integers(0, len(routes), shipment_count)
    service_index = rng.choice(len(services), shipment_count, p=[0.18, 0.57, 0.25])
    start = np.datetime64("2026-01-01T00:00:00")
    day_offsets = rng.integers(0, 30, shipment_count)
    hour_offsets = rng.integers(6, 20, shipment_count)
    created_ts = (
        start + day_offsets.astype("timedelta64[D]") + hour_offsets.astype("timedelta64[h]")
    )
    sla_days = services.iloc[service_index]["sla_days"].to_numpy(dtype=np.int16)
    promised_ts = created_ts + sla_days.astype("timedelta64[D]")
    weight = np.round(
        np.clip(rng.lognormal(mean=1.5, sigma=0.8, size=shipment_count), 0.1, 31.5), 2
    )
    shipment_ids = np.array([f"SHP-{value:09d}" for value in range(1, shipment_count + 1)])

    chosen_routes = routes.iloc[route_index].reset_index(drop=True)
    shipments = pd.DataFrame(
        {
            "shipment_id": shipment_ids,
            "created_ts": pd.to_datetime(created_ts),
            "promised_delivery_ts": pd.to_datetime(promised_ts),
            "origin_hub_id": chosen_routes["origin_hub_id"],
            "destination_hub_id": chosen_routes["destination_hub_id"],
            "route_id": chosen_routes["route_id"],
            "service_level_id": services.iloc[service_index]["service_level_id"].to_numpy(),
            "customer_region": rng.choice(
                ["NORTH", "EAST", "SOUTH", "WEST", "CENTRAL"], shipment_count
            ),
            "weight_kg": weight,
        }
    )

    defect_counts = {
        name: _scaled_count(target, shipment_count)
        for name, target in DEFAULT_DEFECT_TARGETS.items()
    }
    shipment_permutation = rng.permutation(shipment_count)
    cursor = 0
    negative_rows = shipment_permutation[
        cursor : cursor + defect_counts["shipment_negative_weight"]
    ]
    cursor += len(negative_rows)
    unknown_route_rows = shipment_permutation[
        cursor : cursor + defect_counts["shipment_unknown_route"]
    ]
    shipments.loc[negative_rows, "weight_kg"] = -shipments.loc[negative_rows, "weight_kg"].abs()
    shipments.loc[unknown_route_rows, "route_id"] = "R-UNKNOWN"

    sequence = np.tile(np.arange(1, 6, dtype=np.int8), shipment_count)
    repeated_created = np.repeat(created_ts, 5)
    shipment_delay_hours = np.clip(
        rng.normal(loc=3.0, scale=16.0, size=shipment_count).round().astype(np.int16), -18, 60
    )
    duration_hours = np.maximum(12, sla_days.astype(np.int32) * 24 + shipment_delay_hours)
    repeated_duration = np.repeat(duration_hours, 5)
    fractions = np.tile(np.array([0.0, 0.08, 0.18, 0.72, 1.0]), shipment_count)
    event_hours = np.rint(repeated_duration * fractions).astype(np.int32)
    event_ts = repeated_created + event_hours.astype("timedelta64[h]")
    event_type = np.tile(EVENT_TYPES, shipment_count)
    repeated_origin = np.repeat(chosen_routes["origin_hub_id"].to_numpy(), 5)
    repeated_destination = np.repeat(chosen_routes["destination_hub_id"].to_numpy(), 5)
    hub_id = np.where(sequence <= 3, repeated_origin, repeated_destination)
    repeated_shipments = np.repeat(shipment_ids, 5)
    event_ids = np.array(
        [
            f"EV-{shipment:09d}-{seq}"
            for shipment in range(1, shipment_count + 1)
            for seq in range(1, 6)
        ]
    )
    events = pd.DataFrame(
        {
            "event_id": event_ids,
            "shipment_id": repeated_shipments,
            "event_sequence": sequence,
            "event_type": event_type,
            "event_ts": pd.to_datetime(event_ts),
            "hub_id": hub_id,
        }
    )
    events["arrival_batch_date"] = events["event_ts"].dt.normalize()

    invalid_parent_shipments = set(
        shipments.loc[np.r_[negative_rows, unknown_route_rows], "shipment_id"]
    )
    eligible_event_rows = events.index[
        ~events["shipment_id"].isin(invalid_parent_shipments)
    ].to_numpy()
    event_permutation = rng.permutation(eligible_event_rows)
    cursor = 0
    defect_rows: dict[str, np.ndarray] = {}
    for defect_name in [
        "event_null_timestamp",
        "event_unknown_hub",
        "event_unknown_shipment",
        "event_invalid_sequence",
        "late_arriving_valid_event",
        "event_duplicate",
    ]:
        count = defect_counts[defect_name]
        defect_rows[defect_name] = event_permutation[cursor : cursor + count]
        cursor += count

    events.loc[defect_rows["event_null_timestamp"], "event_ts"] = pd.NaT
    events.loc[defect_rows["event_unknown_hub"], "hub_id"] = "H-UNKNOWN"
    events.loc[defect_rows["event_unknown_shipment"], "shipment_id"] = "SHP-UNKNOWN"
    invalid_rows = defect_rows["event_invalid_sequence"]
    current_types = events.loc[invalid_rows, "event_type"].to_numpy()
    events.loc[invalid_rows, "event_type"] = np.where(
        current_types == "DELIVERED", "CREATED", "DELIVERED"
    )
    events.loc[defect_rows["late_arriving_valid_event"], "arrival_batch_date"] += pd.Timedelta(
        days=2
    )

    duplicates = events.loc[defect_rows["event_duplicate"]].copy()
    events = pd.concat([events, duplicates], ignore_index=True)
    events["arrival_batch_date"] = events["arrival_batch_date"].dt.strftime("%Y-%m-%d")

    _write_parquet(shipments, output_dir / "source" / "shipments" / "shipments.parquet")
    _write_parquet(hubs, output_dir / "source" / "reference" / "hubs.parquet")
    _write_parquet(routes, output_dir / "source" / "reference" / "routes.parquet")
    _write_parquet(services, output_dir / "source" / "reference" / "service_levels.parquet")
    for batch_date, batch in events.groupby("arrival_batch_date", sort=True):
        payload = batch.drop(columns="arrival_batch_date").reset_index(drop=True)
        _write_parquet(
            payload,
            output_dir
            / "source"
            / "shipment_events"
            / f"batch_date={batch_date}"
            / "events.parquet",
        )

    files = sorted(output_dir.rglob("*.parquet"))
    manifest: dict[str, Any] = {
        "dataset_name": "nordlicht_logistics_synthetic_v1",
        "fictional_data": True,
        "seed": seed,
        "shipment_count": shipment_count,
        "base_event_count": shipment_count * 5,
        "event_row_count_including_duplicates": len(events),
        "batch_count": events["arrival_batch_date"].nunique(),
        "expected_counts": defect_counts,
        "schemas": {
            "shipments": list(shipments.columns),
            "events": [column for column in events.columns if column != "arrival_batch_date"],
        },
        "files": [
            {
                "path": path.relative_to(output_dir).as_posix(),
                "sha256": _sha256(path),
                "bytes": path.stat().st_size,
            }
            for path in files
        ],
        "generated_at_note": (
            "Omitted intentionally so identical inputs yield identical manifest content."
        ),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return GenerationResult(manifest_path, shipment_count, len(events), len(files))


def manifest_fingerprint(manifest_path: Path) -> str:
    """Return a stable fingerprint over logical generation inputs and file hashes."""
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    stable = {
        "seed": payload["seed"],
        "shipment_count": payload["shipment_count"],
        "expected_counts": payload["expected_counts"],
        "files": payload["files"],
    }
    return hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()
