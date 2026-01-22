#!/usr/bin/env bash
set -euo pipefail

# -------------------------------------------------------------------
# Paths (adjust only if your repo structure changes)
# -------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

PARSERS_DIR="$ROOT_DIR/app/infrastructure/parsers"
SCRAPERS_DIR="$ROOT_DIR/app/infrastructure/scrapers"

EXTRACT_CALLS="$PARSERS_DIR/extract_horizon_cl_calls.py"
BUILD_GROUPED="$PARSERS_DIR/build_calls_grouped_by_destination.py"
SPLIT_CLUSTERS="$PARSERS_DIR/split_calls_by_cluster.py"

FETCH_METADATA="$SCRAPERS_DIR/fetch_call_metadata.py"

# -------------------------------------------------------------------
# Inputs / outputs
# -------------------------------------------------------------------

FACET_JSON="${1:-$ROOT_DIR/data/response_body_2026.json}"
OUT_DIR="${2:-$ROOT_DIR/data/out_2026_2027}"

CALL_IDS_JSON="$OUT_DIR/horizon_cl_all_2026_2027_calls.json"
FETCHED_METADATA_JSON="$OUT_DIR/fetched_call_metadata_2026_2027.json"
GROUPED_BY_DEST_JSON="$OUT_DIR/calls_grouped_by_destination_2026_2027.json"

mkdir -p "$OUT_DIR"

# -------------------------------------------------------------------
# 1) Extract topic IDs from FACET API response
# -------------------------------------------------------------------

python "$EXTRACT_CALLS" \
  --input "$FACET_JSON" \
  --out "$CALL_IDS_JSON" \
  --years 2026 2027 \
  --clusters 1 2 3 4 5 6

# -------------------------------------------------------------------
# 2) Fetch authoritative metadata per topic (Search API)
# -------------------------------------------------------------------

python "$FETCH_METADATA" \
  --input "$CALL_IDS_JSON" \
  --out "$FETCHED_METADATA_JSON"

# -------------------------------------------------------------------
# 3) Group calls by destination (authoritative metadata.destinationDescription)
# -------------------------------------------------------------------

python "$BUILD_GROUPED" \
  --input "$FETCHED_METADATA_JSON" \
  --out "$GROUPED_BY_DEST_JSON"

# -------------------------------------------------------------------
# 4) Split into per-cluster V0-compatible grouped JSONs
# -------------------------------------------------------------------

python "$SPLIT_CLUSTERS" \
  --input-file "$GROUPED_BY_DEST_JSON" \
  --out-dir "$OUT_DIR" \
  --filename-template "cluster_{cluster}.grouped.json" \
  --write-destination-summaries

echo "DONE: pipeline completed successfully"
echo "Output directory: $OUT_DIR"
