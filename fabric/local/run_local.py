"""Run the Fabric notebook sources unchanged in a local Spark + Delta container.

The container mounts the bronze files at the same /lakehouse/default/Files path that a
Fabric notebook sees for its default Lakehouse. Each notebook runs as in a pipeline: the
parameter cell first, then the injected parameters, then the remaining cells.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

NOTEBOOK_DIR = Path(__file__).resolve().parents[1] / "notebooks"
NOTEBOOKS = ["01_ingest_bronze", "02_build_silver", "03_build_gold", "04_validate"]
EVIDENCE = Path(__file__).resolve().parents[2] / "data" / "evidence" / "local_spark_run.json"


def cells(path: Path) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    header, lines = "", []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# %%"):
            found.append((header, "\n".join(lines)))
            header, lines = line, []
        else:
            lines.append(line)
    found.append((header, "\n".join(lines)))
    return found


def run_notebook(spark: SparkSession, name: str, parameters: dict[str, object]) -> dict:
    namespace: dict[str, object] = {"spark": spark, "__name__": "__notebook__"}
    for header, source in cells(NOTEBOOK_DIR / f"{name}.py"):
        exec(compile(source, f"{name}.py", "exec"), namespace)
        if "parameters" in header:
            namespace.update(parameters)
    # Fabric runs every notebook activity in its own session, so nothing temporary may leak.
    for table in spark.sql("SHOW TABLES").collect():
        if table.isTemporary:
            spark.catalog.dropTempView(table.tableName)
    spark.catalog.clearCache()
    return namespace


def main() -> int:
    spark = SparkSession.builder.appName("fabric-notebooks-local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    summaries = []
    for attempt in (1, 2):
        run_id = f"local-spark-run-{attempt}"
        for name in NOTEBOOKS:
            print(f"== run {attempt}: {name}", flush=True)
            parameters: dict[str, object] = {"run_id": run_id}
            if name == "04_validate":
                parameters["execution_scope"] = "LOCAL_SPARK_DELTA_CONTAINER"
            namespace = run_notebook(spark, name, parameters)
        summaries.append(namespace["summary"])

    first, second = summaries
    idempotent = (
        second["loaded_files"] == 0 and second["skipped_files"] == first["loaded_files"]
    )
    evidence = {"runs": summaries, "second_run_skipped_all_files": idempotent}
    EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2, sort_keys=True))
    passed = idempotent and all(summary["status"] == "SUCCESS" for summary in summaries)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
