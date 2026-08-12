from __future__ import annotations

import argparse
import json
from pathlib import Path

from supply_chain_ops.config import DEFAULT_SEED, DEFAULT_SHIPMENT_COUNT, ProjectPaths
from supply_chain_ops.generator import generate_dataset, manifest_fingerprint


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Supply-chain analytics lakehouse")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic bronze files")
    generate.add_argument("--shipments", type=int, default=DEFAULT_SHIPMENT_COUNT)
    generate.add_argument("--seed", type=int, default=DEFAULT_SEED)
    generate.add_argument("--force", action="store_true")

    subparsers.add_parser("run", help="run the local bronze-to-gold pipeline")
    subparsers.add_parser("status", help="show local dataset and pipeline status")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    paths = ProjectPaths(args.data_root)
    if args.command == "generate":
        result = generate_dataset(paths.bronze, args.shipments, args.seed, args.force)
        print(
            json.dumps(
                {
                    "manifest": str(result.manifest_path),
                    "fingerprint": manifest_fingerprint(result.manifest_path),
                    "shipments": result.shipment_count,
                    "event_rows": result.event_row_count,
                    "parquet_files": result.file_count,
                },
                indent=2,
            )
        )
        return 0
    if args.command == "run":
        from supply_chain_ops.pipeline import run_pipeline

        print(json.dumps(run_pipeline(paths), indent=2, default=str))
        return 0
    if args.command == "status":
        status = {
            "manifest_exists": (paths.bronze / "manifest.json").exists(),
            "database_exists": paths.database.exists(),
            "evidence_exists": (paths.evidence / "latest_run.json").exists(),
        }
        print(json.dumps(status, indent=2))
        return 0
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
