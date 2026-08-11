param(
    [string]$Profile = "jagadeeswaran",
    [string]$VolumePath = "dbfs:/Volumes/stock_project/market_data/pipeline_files",
    [switch]$Overwrite
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$downloads = Join-Path $projectRoot "downloads"

if (-not (Test-Path $downloads -PathType Container)) {
    throw "Downloads folder not found: $downloads"
}

$flags = @("--profile", $Profile, "--recursive")
if ($Overwrite) { $flags += "--overwrite" }

& databricks fs cp $downloads "$VolumePath/downloads" @flags
