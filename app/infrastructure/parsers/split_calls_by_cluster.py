#!/usr/bin/env python3
"""
split_calls_by_cluster.py

Reads `calls_grouped_by_destination.json` (API-based data grouped by Destination)
and writes:
  1) one JSON file per cluster (CL2, CL3, CL4, ...), shaped like your graph loaders expect:
     [ { "destination": <name>, "calls": [ ... ] }, ... ]
  2) one "destination summaries" JSON per cluster that contains:
     - one cluster title object
     - one entry per destination with an empty summary (to fill later)

Usage (Windows PowerShell example):
  python app/infrastructure/scrapers/split_calls_by_cluster.py `
    --input-file app/data/calls_grouped_by_destination.json `
    --out-dir app/data `
    --write-cl2-compat

Notes:
- Cluster title mapping is provided for CL2–CL6. Unknown clusters fall back to "Cluster <n>".
- Destination summary keys are the *exact* destination names we write into the calls file,
  so your builder can look them up directly.
"""

import argparse
import json
import os
import re
from collections import defaultdict

# --------- CONFIG: human-readable cluster titles (extend as needed) ----------
# In split_calls_by_cluster.py

CLUSTER_TITLES = {
    "CL1": "Cluster 1 - Health",
    "CL2": "Cluster 2 - Culture, Creativity and Inclusive Society",
    "CL3": "Cluster 3 - Civil Security for Society",
    "CL4": "Cluster 4 - Digital, Industry and Space",
    "CL5": "Cluster 5 - Climate, Energy and Mobility",
    "CL6": "Cluster 6 - Food, Bioeconomy, Natural Resources, Agriculture and Environment",
}


# OLD:
# TOPIC_ID_PATTERN = re.compile(r'HORIZON-CL\d-\d{4}-\d{2}-[A-Z0-9-]+')
# NEW:
TOPIC_ID_PATTERN = re.compile(r'HORIZON-CL\d-\d{4}-\d{2}-[A-Za-z0-9-]+')

# --- add near the top ---
def normalize_call_id_for_v0(call_id: str) -> str:
    if not call_id:
        return call_id
    # CL4 abbreviation normalization (extend with more rules if needed)
    call_id = call_id.replace("-MAT-PROD-", "-MATERIALS-PRODUCTION-")
    # If your DB expects no '-two-stage' suffix, uncomment:
    # if call_id.endswith("-two-stage"):
    #     call_id = call_id[:-10]
    return call_id

def to_v0_shape(dest_map: dict) -> dict:
    # dest_map: { dest_name: {"destination": name, "calls":[...]}, ... }
    destinations = []
    for dest_name in sorted(dest_map.keys()):
        bucket = dest_map[dest_name]
        calls = []
        for c in bucket["calls"]:
            c = dict(c)
            # preserve traceability
            c["original_call_id"] = c.get("call_id", "")
            c["call_id"] = normalize_call_id_for_v0(c.get("call_id", ""))
            calls.append(c)

        destinations.append({
            "destination_title": bucket["destination"],
            "calls": calls
        })

    return {"destinations": destinations}

def infer_call_id(item):
    """
    Try to get a proper topic id like HORIZON-CL2-2025-01-DEMOCRACY-01.
    Fallbacks:
      - from 'identifier'
      - from 'url' (last segment before .json)
      - from 'keywords' array (first matching topic pattern)
    """
    ident = (item.get("identifier") or "").strip()
    if TOPIC_ID_PATTERN.fullmatch(ident):
        return ident

    url = (item.get("url") or "").strip()
    if url:
        last = url.rstrip("/").split("/")[-1]
        if last.endswith(".json"):
            last = last[:-5]
        if TOPIC_ID_PATTERN.fullmatch(last):
            return last

    for kw in item.get("keywords", []):
        kw = (kw or "").strip()
        if TOPIC_ID_PATTERN.fullmatch(kw):
            return kw

    return ident or None


def infer_cluster_code(call_id, item):
    """Extract CL2/CL3/etc. from the topic id. Fallback to budget.action or keywords."""
    if call_id:
        m = re.search(r'HORIZON-(CL\d)-', call_id)
        if m:
            return m.group(1).upper()

    action = (item.get("budget", {}) or {}).get("action") or ""
    m = re.search(r'HORIZON-(CL\d)-', action)
    if m:
        return m.group(1).upper()

    for kw in item.get("keywords", []):
        m = re.search(r'HORIZON-(CL\d)-', kw or "")
        if m:
            return m.group(1).upper()

    return None


def normalize_type_of_action(s):
    """Normalize type string; drop leading 'HORIZON' noise and collapse whitespace."""
    if not s:
        return ""
    s = " ".join(str(s).split())
    if s.upper().startswith("HORIZON"):
        parts = s.split()
        s = " ".join(parts[1:]) if len(parts) > 1 else "HORIZON"
    return s


def make_funding_link(call_id):
    """Funding & Tenders topic-details link from the topic id."""
    if not call_id:
        return ""
    base = "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/"
    return base + call_id


def build_call_record(item, dest_name):
    """
    Map API fields to the schema expected by your cluster loaders (CL2 reference).
    Keeps your existing budget/status/deadline mapping logic.
    """
    call_id = infer_call_id(item)
    call_section = item.get("call_title") or ""
    type_of_action = normalize_type_of_action(item.get("type_of_action"))
    budget = item.get("budget") or {}

    # Core budgetary fields + fallbacks (as in your current version):
    expected_grants = budget.get("expectedGrants")
    min__contribution = budget.get("minContribution")
    max__contribution = budget.get("maxContribution")
    indicative_budget = budget.get("maxContribution")  # kept consistent with your previous script

    # Status & deadline model (prefer item-level, then budget)
    status = item.get("status") or (budget.get("status") if isinstance(budget.get("status"), str) else "")
    deadline_model = item.get("deadline_model") or budget.get("deadlineModel") or ""

    # Dates: prefer item-level; fallback to budget fields
    opening_date = item.get("start_date") or budget.get("plannedOpeningDate") or ""
    deadline = item.get("deadline_date") or (
        ",".join(budget.get("deadlineDates", [])) if budget.get("deadlineDates") else ""
    )

    rec = {
        "call_id": call_id or "",
        "call_title": item.get("topic_title") or "",
        "call_type": type_of_action,
        "call_section": call_section,
        "min__contribution": min__contribution,
        "max_contribution": max__contribution,
        "expected_eu_contribution": str(min__contribution) + " - " + str(max__contribution),
        "indicative_budget": (indicative_budget * expected_grants) if (indicative_budget is not None and isinstance(expected_grants, int)) else "",
        "type_of_action": type_of_action,
        "admissibility_conditions": "",
        "eligibility_conditions": "",
        "technology_readiness_level": "",
        "procedure": "",
        "legal_and_financial_setup": "",
        "exceptional_page_limits": "",
        "expected_outcome": item.get("expected_outcome") or "",
        "scope": item.get("scope") or "",
        "destination": dest_name or "",
        "funding_link": make_funding_link(call_id),
        "max_funded_projects": expected_grants if isinstance(expected_grants, int) else None,
        "opening_date": opening_date,
        "deadline": deadline,
        "status": status,
        "deadline_model": deadline_model,
    }
    return rec


def build_destination_summaries_payload(cluster_code: str, destination_names):
    """
    Build the summaries JSON object:
      {
        "<Cluster title>": {"summary": ""},
        "<Destination 1>": {"summary": ""},
        "<Destination 2>": {"summary": ""},
        ...
      }
    """
    # Human-readable cluster title, fallback if unknown:
    title = CLUSTER_TITLES.get(cluster_code)
    if not title:
        # Try to build "Cluster <n>" if cluster_code like "CL7"
        m = re.match(r"CL(\d+)", cluster_code or "")
        title = f"Cluster {m.group(1)}" if m else (cluster_code or "Cluster")

    payload = {title: {"summary": ""}}
    for d in sorted(destination_names):
        payload[d] = {"summary": ""}
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-file", required=True, help="Path to calls_grouped_by_destination.json")
    ap.add_argument("--out-dir", required=True, help="Directory to write per-cluster JSON files")
    ap.add_argument(
        "--filename-template",
        default="cluster_{cluster}.json",
        help="Output filename template (default: cluster_{cluster}.json). {cluster} will be CL2/CL3/etc.",
    )
    ap.add_argument(
        "--write-cl2-compat",
        action="store_true",
        help="Also write updated_nested_parsed_cl2_calls_with_max_funded_projects.json for CL2.",
    )
    ap.add_argument(
        "--dest-summaries-template",
        default="destination_summaries_{cluster_lc}.json",
        help="Destination summaries filename template (default: destination_summaries_{cluster_lc}.json).",
    )
    args = ap.parse_args()

    with open(args.input_file, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Build { CLX: [ { destination, calls: [...] }, ... ] } and collect destinations per cluster
    per_cluster = defaultdict(lambda: defaultdict(lambda: {"destination": "", "calls": []}))
    cluster_dest_names = defaultdict(set)

    for dest_name, items in (raw or {}).items():
        for it in items:
            call_id = infer_call_id(it)
            cluster = infer_cluster_code(call_id, it) or "_UNKNOWN"
            bucket = per_cluster[cluster][dest_name]
            bucket["destination"] = dest_name
            bucket["calls"].append(build_call_record(it, dest_name))
            cluster_dest_names[cluster].add(dest_name)

    os.makedirs(args.out_dir, exist_ok=True)

    written = []
    for cluster, dest_map in per_cluster.items():
        # ---- 1) Write the per-cluster calls file ----
        out_payload = to_v0_shape(dest_map)
        fn = args.filename_template.format(cluster=cluster)
        out_path = os.path.join(args.out_dir, fn)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out_payload, f, indent=2, ensure_ascii=False)
        written.append(out_path)

        # CL2 compatibility file (optional)
        if cluster == "CL2" and args.write_cl2_compat:
            compat_name = "updated_nested_parsed_cl2_calls_with_max_funded_projects.json"
            compat_path = os.path.join(args.out_dir, compat_name)
            with open(compat_path, "w", encoding="utf-8") as f:
                json.dump(out_payload, f, indent=2, ensure_ascii=False)
            written.append(compat_path)

        # ---- 2) Write the destination summaries file for this cluster ----
        summaries_payload = build_destination_summaries_payload(cluster, cluster_dest_names[cluster])
        dest_fn = args.dest_summaries_template.format(cluster=cluster, cluster_lc=cluster.lower())
        dest_path = os.path.join(args.out_dir, dest_fn)
        with open(dest_path, "w", encoding="utf-8") as f:
            json.dump(summaries_payload, f, indent=2, ensure_ascii=False)
        written.append(dest_path)

    print(json.dumps({"ok": True, "written": written, "clusters": sorted(per_cluster.keys())}, ensure_ascii=False))


if __name__ == "__main__":
    main()
