$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$goldRoot = Join-Path $projectRoot "data\gold"
$requiredTables = @(
    "dim_date.parquet",
    "dim_hub.parquet",
    "dim_route.parquet",
    "dim_service_level.parquet",
    "fact_shipments.parquet",
    "fact_events.parquet",
    "fact_data_quality.parquet"
)

$missingTables = @(
    $requiredTables | Where-Object { -not (Test-Path -LiteralPath (Join-Path $goldRoot $_)) }
)

if ($missingTables.Count -gt 0) {
    throw "Gold tables are missing. Run the generator and pipeline before opening Power BI: $($missingTables -join ', ')"
}

$runtimeRoot = Join-Path $projectRoot "powerbi\.runtime"
$generator = Join-Path $projectRoot "scripts\build_powerbi_project.mjs"

& node $generator `
    --output $runtimeRoot `
    --project-name SupplyChainOperationsLocal `
    --data-root (Resolve-Path -LiteralPath $goldRoot).Path

if ($LASTEXITCODE -ne 0) {
    throw "The Power BI project generator failed with exit code $LASTEXITCODE."
}

$reportPath = Join-Path $runtimeRoot "SupplyChainOperationsLocal.pbip"
Write-Host "Opening $reportPath"
Start-Process -FilePath $reportPath
