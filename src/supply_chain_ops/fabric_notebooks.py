from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK_SOURCES = PROJECT_ROOT / "fabric" / "notebooks"
NOTEBOOK_IMPORTS = NOTEBOOK_SOURCES / "import"


def _cells(source: str) -> list[tuple[str, str]]:
    cells: list[tuple[str, str]] = []
    header, lines = "", []
    for line in source.splitlines():
        if line.startswith("# %%"):
            cells.append((header, "\n".join(lines).strip("\n")))
            header, lines = line, []
        else:
            lines.append(line)
    cells.append((header, "\n".join(lines).strip("\n")))
    return cells


def _lines(text: str) -> list[str]:
    parts = text.split("\n")
    return [line + "\n" for line in parts[:-1]] + [parts[-1]]


def to_ipynb(source: str) -> dict:
    """Convert a percent-format notebook source into a Fabric-importable Jupyter notebook."""
    cells = []
    for header, body in _cells(source):
        if not header:
            comments = [line.removeprefix("#").strip() for line in body.splitlines()]
            markdown = _lines("\n".join(comments))
            cells.append({"cell_type": "markdown", "metadata": {}, "source": markdown})
            continue
        metadata = {"tags": ["parameters"]} if "parameters" in header else {}
        cells.append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": metadata,
                "outputs": [],
                "source": _lines(body),
            }
        )
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "Python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }


def render_notebooks() -> dict[str, str]:
    rendered = {}
    for path in sorted(NOTEBOOK_SOURCES.glob("[0-9][0-9]_*.py")):
        notebook = to_ipynb(path.read_text(encoding="utf-8"))
        rendered[f"{path.stem}.ipynb"] = json.dumps(notebook, indent=1, ensure_ascii=False) + "\n"
    return rendered


def write_notebooks() -> list[Path]:
    NOTEBOOK_IMPORTS.mkdir(parents=True, exist_ok=True)
    written = []
    for name, content in render_notebooks().items():
        destination = NOTEBOOK_IMPORTS / name
        destination.write_text(content, encoding="utf-8", newline="\n")
        written.append(destination)
    return written
