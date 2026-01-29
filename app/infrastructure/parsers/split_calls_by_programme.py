#!/usr/bin/env python3
"""
split_calls_by_programme.py

Convert a grouped dict (group_value -> list[normalized call items])
into the V0-shaped grouped JSON that your DB expects:

{
  "destinations": [
    { "destination_title": "<group_value>", "calls": [ {call fields...}, ... ] },
    ...
  ]
}

Writes to <out-dir>/<programme>.grouped.json (e.g., DEP.grouped.json)
and optionally destination_summaries_<programme>.json.

Important for DEP/DIGITAL:
- `metadata.identifier` is NOT unique across Search API records.
- We therefore dedupe (if needed) by `unique_key` produced by build_calls_grouped.py, or by a fallback composite key derived from raw metadata.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List, Optional


def _first(x: Any, default: Any = "") -> Any:
    if isinstance(x, list):
        return x[0] if x else default
    return x if x is not None else default


def _safe_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (str, int, float, bool)):
        return str(x)
    return ""


def stable_item_key(item: Dict[str, Any]) -> str:
    """Return a stable uniqueness key for a *normalized* item.

    Preference order:
    1) item['unique_key'] (produced by build_calls_grouped.py)
    2) composite of (identifier, callccm2Id, language) from item['raw']['metadata']
    3) composite of (identifier, url)
    """
    uk = _safe_str(item.get("unique_key")).strip()
    if uk:
        return uk

    raw = item.get("raw") or {}
    meta = raw.get("metadata") or {}

    ident = _safe_str(_first(meta.get("identifier"), "")).strip() or _safe_str(item.get("identifier")).strip()
    callccm2 = _safe_str(_first(meta.get("callccm2Id"), "")).strip()
    lang = _safe_str(_first(meta.get("language"), "")).strip()

    if ident and callccm2:
        return f"{ident}|{callccm2}|{lang or 'und'}"

    url = _safe_str(item.get("url")).strip() or _safe_str(_first(meta.get("url"), "")).strip()
    if ident and url:
        return f"{ident}|{url}"

    # last resort (should be rare)
    return ident or url or json.dumps(item, sort_keys=True)


def build_call_record(item: Dict[str, Any], group_value: str) -> Dict[str, Any]:
    """Map a normalized item into the V0 call shape.

    For DEP/DIGITAL we set `call_id` to a stable unique key (NOT `identifier`),
    because `identifier` repeats across distinct items.
    """
    raw = item.get("raw") or {}
    meta = raw.get("metadata") or {}

    call_id = stable_item_key(item)
    funding_link = (_safe_str(item.get("url")) or _safe_str(_first(meta.get("url"), ""))).strip()

    out = {
        # Unique id for DB/graph purposes
        "call_id": call_id,

        # Helpful traceability fields (do not rely on these for uniqueness)
        "topic_identifier": (_safe_str(item.get("identifier")) or _safe_str(_first(meta.get("identifier"), ""))).strip(),
        "callccm2Id": (_safe_str(_first(meta.get("callccm2Id"), ""))).strip(),
        "callIdentifier": (_safe_str(_first(meta.get("callIdentifier"), ""))).strip(),

        "call_title": item.get("call_title") or item.get("topic_title") or "",
        "type_of_action": item.get("type_of_action") or "",

        # contributions/budget
        "expected_eu_contribution": "",
        "min__contribution": float(item.get("min_contribution") or 0.0),
        "max_contribution": float(item.get("max_contribution") or 0.0),
        "indicative_budget": float(item.get("indicative_budget") or 0.0),
        "indicative_number_of_projects": item.get("indicative_number_of_projects"),

        # conditions & admin
        "admissibility_conditions": item.get("admissibility_conditions") or "",
        "eligibility_conditions": item.get("eligibility_conditions") or "",
        "technology_readiness_level": item.get("technology_readiness_level") or "",
        "procedure": item.get("procedure") or "",
        "legal_and_financial_setup": item.get("legal_and_financial_setup") or "",
        "exceptional_page_limits": item.get("exceptional_page_limits") or "",

        # content
        "expected_outcome": item.get("expected_outcome") or "",
        "scope": item.get("scope") or "",

        # dates/status
        "opening_date": item.get("opening_date") or "",
        "deadline": item.get("deadline") or "",
        "status": item.get("status") or "",
        "deadline_model": item.get("deadline_model") or "",

        # links/taxonomy
        "funding_link": funding_link,
        "destination": group_value,
        "tags": item.get("tags") or [],
        "keywords": item.get("keywords") or [],
    }
    return out


def to_v0_shape(grouped: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    destinations = []
    for group_value in sorted(grouped.keys()):
        destinations.append({"destination_title": group_value, "calls": grouped[group_value]})
    return {"destinations": destinations}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-file", required=True, help="Grouped JSON dict: group_value -> list[normalized items]")
    ap.add_argument("--out-dir", required=True, help="Directory to write outputs")
    ap.add_argument("--programme", required=True, help="Programme short code for filename, e.g. DEP")
    ap.add_argument("--write-destination-summaries", action="store_true")
    ap.add_argument("--programme-title", default="", help="Optional: descriptive programme title for summaries")
    args = ap.parse_args()

    raw = json.loads(open(args.input_file, "r", encoding="utf-8").read())
    if not isinstance(raw, dict):
        raise SystemExit("ERROR: input must be a JSON dict (group_value -> list[items])")

    per_group_calls: Dict[str, List[Dict[str, Any]]] = {}
    seen_keys: set[str] = set()
    kept = 0
    dropped_dupes = 0

    for group_value, items in raw.items():
        if not isinstance(items, list):
            continue

        out_calls: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue

            key = stable_item_key(item)
            if not key:
                continue
            if key in seen_keys:
                dropped_dupes += 1
                continue
            seen_keys.add(key)

            out_calls.append(build_call_record(item, group_value))
            kept += 1

        if out_calls:
            per_group_calls[group_value] = out_calls

    os.makedirs(args.out_dir, exist_ok=True)

    out_payload = to_v0_shape(per_group_calls)
    out_path = os.path.join(args.out_dir, f"{args.programme}.grouped.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_payload, f, indent=2, ensure_ascii=False)

    if args.write_destination_summaries:
        prog_title = args.programme_title or args.programme
        summaries = [{"programme": args.programme, "programme_title": prog_title}]
        for d in sorted(per_group_calls.keys()):
            summaries.append({"destination_title": d, "summary": ""})
        summ_path = os.path.join(args.out_dir, f"destination_summaries_{args.programme.lower()}.json")
        with open(summ_path, "w", encoding="utf-8") as f:
            json.dump(summaries, f, indent=2, ensure_ascii=False)

    print(
        f"OK: wrote {out_path} ({len(per_group_calls)} groups, {kept} calls; dropped {dropped_dupes} dupes by stable key)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
