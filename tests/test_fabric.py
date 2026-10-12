from __future__ import annotations

import json
from pathlib import Path

from supply_chain_ops.config import ProjectPaths
from supply_chain_ops.fabric_notebooks import (
    NOTEBOOK_IMPORTS,
    NOTEBOOK_SOURCES,
    render_notebooks,
)
from supply_chain_ops.generator import generate_dataset
from supply_chain_ops.pipeline import run_pipeline
from supply_chain_ops.reference import build_reference


def test_reference_matches_local_pipeline(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path / "data")
    generate_dataset(paths.bronze, shipment_count=1_000, seed=11)
    summary = run_pipeline(paths)

    reference = build_reference(paths)
    checks = reference["checks"]

    for table, count in summary["table_counts"].items():
        assert checks[f"rows.{table}"]["expected"] == count
    for rule, count in summary["quality_counts"].items():
        assert checks[f"quality.{rule}"]["expected"] == count
    assert checks["kpi.late_arriving_events"]["expected"] > 0
    saved = json.loads((paths.evidence / "local_reference.json").read_text(encoding="utf-8"))
    assert saved == reference


def test_fabric_notebook_sources_compile() -> None:
    sources = sorted(NOTEBOOK_SOURCES.glob("[0-9][0-9]_*.py"))
    assert [path.stem for path in sources] == [
        "01_ingest_bronze",
        "02_build_silver",
        "03_build_gold",
        "04_validate",
    ]
    for path in sources:
        compile(path.read_text(encoding="utf-8"), str(path), "exec")


def test_fabric_import_notebooks_are_current() -> None:
    rendered = render_notebooks()
    for name, content in rendered.items():
        on_disk = (NOTEBOOK_IMPORTS / name).read_text(encoding="utf-8")
        assert on_disk == content, f"{name} is stale; rerun the fabric-notebooks command"
        code_cells = [cell for cell in json.loads(content)["cells"] if cell["cell_type"] == "code"]
        assert code_cells[0]["metadata"] == {"tags": ["parameters"]}
