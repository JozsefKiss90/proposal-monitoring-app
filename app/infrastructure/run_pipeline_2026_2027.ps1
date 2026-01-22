# app\infrastructure\run_pipeline_2026_2027.ps1
# Run from anywhere:
#   powershell -ExecutionPolicy Bypass -File .\run_pipeline_2026_2027.ps1

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Run-Step {
    param(
        [Parameter(Mandatory=$true)][string]$Cmd,
        [Parameter(Mandatory=$true)][string]$StepName
    )

    Write-Host "`n=== $StepName ==="
    Write-Host $Cmd

    cmd /c $Cmd
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed ($StepName) with exit code $LASTEXITCODE"
    }
}

# -------------------------------------------------------------------
# Correct repo/app root (your data is under app\data)
# PSScriptRoot = ...\app\infrastructure
# APP_DIR      = ...\app
# -------------------------------------------------------------------
$APP_DIR = (Resolve-Path "$PSScriptRoot\..").Path

$PARSERS_DIR  = Join-Path $APP_DIR "infrastructure\parsers"
$SCRAPERS_DIR = Join-Path $APP_DIR "infrastructure\scrapers"

$EXTRACT_CALLS  = Join-Path $PARSERS_DIR  "extract_horizon_cl_calls.py"
$BUILD_GROUPED  = Join-Path $PARSERS_DIR  "build_calls_grouped_by_destination.py"
$SPLIT_CLUSTERS = Join-Path $PARSERS_DIR  "split_calls_by_cluster.py"
$FETCH_METADATA = Join-Path $SCRAPERS_DIR "fetch_call_metadata.py"

# -------------------------------------------------------------------
# Destination/Call-ID mapping JSONs (kept in app\infrastructure\parsers\jsons)
# -------------------------------------------------------------------
$JSONS_DIR = Join-Path $PARSERS_DIR "jsons"

$CL1_MAP = Join-Path $JSONS_DIR "c1_destinations_call_ids.json"
$CL2_MAP = Join-Path $JSONS_DIR "cl2_destinations_call_ids.json"
$CL3_MAP = Join-Path $JSONS_DIR "cl3_destinations_call_ids.json"

# -------------------------------------------------------------------
# Inputs / outputs 
# -------------------------------------------------------------------
$FACET_JSON = Join-Path $APP_DIR "data\response_body_2026.json"

# Choose ONE output folder:
# Option A (new folder):
$OUT_DIR = Join-Path $APP_DIR "data\out_2026_2027"
# Option B (match your existing structure):
# $OUT_DIR = Join-Path $APP_DIR "data\2026_2027"

$CALL_IDS_JSON         = Join-Path $OUT_DIR "horizon_cl_all_2026_2027_calls.json"
$FETCHED_METADATA_JSON = Join-Path $OUT_DIR "fetched_call_metadata_2026_2027.json"
$GROUPED_BY_DEST_JSON  = Join-Path $OUT_DIR "calls_grouped_by_destination_2026_2027.json"

New-Item -ItemType Directory -Force -Path $OUT_DIR | Out-Null

# -------------------------------------------------------------------
# 1) Extract topic IDs (inject CL1 IDs via --cl1-map)
# -------------------------------------------------------------------
Run-Step `
  -StepName "1) Extract topic IDs from FACET JSON (+ inject CL1)" `
  -Cmd "python `"$EXTRACT_CALLS`" --input `"$FACET_JSON`" --out `"$CALL_IDS_JSON`" --years 2026 2027 --clusters 1 2 3 4 5 6 --cl1-map `"$CL1_MAP`""

# -------------------------------------------------------------------
# 2) Fetch metadata (Search API)
# -------------------------------------------------------------------
Run-Step ` 
  -StepName "2) Fetch Search API metadata per topic" `
  -Cmd "python `"$FETCH_METADATA`" --input `"$CALL_IDS_JSON`" --out `"$FETCHED_METADATA_JSON`""

# -------------------------------------------------------------------
# 3) Build grouped-by-destination JSON (backfill CL2/CL3/CL1 destinations)
# -------------------------------------------------------------------
Run-Step `
  -StepName "3) Build calls_grouped_by_destination_2026_2027.json (+ destination backfills)" `
  -Cmd "python `"$BUILD_GROUPED`" --input `"$FETCHED_METADATA_JSON`" --out `"$GROUPED_BY_DEST_JSON`" --cl2-map `"$CL2_MAP`" --cl3-map `"$CL3_MAP`" --cl1-map `"$CL1_MAP`""

# -------------------------------------------------------------------
# 4) Split into per-cluster grouped JSONs (includes CL1 via HLTH IDs)
# -------------------------------------------------------------------
Run-Step `
  -StepName "4) Split into cluster_CLx.grouped.json outputs" `
  -Cmd "python `"$SPLIT_CLUSTERS`" --input-file `"$GROUPED_BY_DEST_JSON`" --out-dir `"$OUT_DIR`" --filename-template `"cluster_{cluster}.grouped.json`" --write-destination-summaries"

Write-Host "`nDONE: pipeline completed successfully"
Write-Host "Output directory: $OUT_DIR"
