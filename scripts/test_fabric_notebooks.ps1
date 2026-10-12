# Runs the Fabric notebook sources twice in a local Spark + Delta container and reconciles
# them with the DuckDB reference. Requires Docker and a completed local pipeline run.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$image = "sco-fabric-local"

& "$root\.venv\Scripts\python.exe" -m supply_chain_ops --data-root "$root\data" reference
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker build -t $image "$root\fabric\local"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

docker run --rm `
    --memory 2500m `
    -v "${root}:/work" `
    -v "${root}\data\bronze:/lakehouse/default/Files/bronze:ro" `
    -v "${root}\data\evidence\local_reference.json:/lakehouse/default/Files/reference/local_reference.json:ro" `
    -v "sco-ivy-cache:/root/.ivy2" `
    -w /lakehouse/default `
    $image `
    spark-submit `
    --master "local[2]" `
    --driver-memory 1500m `
    --packages io.delta:delta-spark_2.12:3.2.0 `
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
    --conf spark.sql.warehouse.dir=/lakehouse/default/Tables `
    --conf spark.ui.enabled=false `
    --conf spark.sql.shuffle.partitions=8 `
    /work/fabric/local/run_local.py
exit $LASTEXITCODE
