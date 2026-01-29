#!/usr/bin/env python3
"""
build_calls_grouped.py

Group call/topic records into:
  { group_value -> [normalized_items...] }

Key fix:
- Works with BOTH:
  A) Search API results (top-level "metadata")
  B) fetched topic metadata items (your pillar_*_metadata.json) where metadata lives at raw.metadata

For programme=HORIZON:
- Pillar 2 (Clusters + Missions): group strictly by the *group code embedded in the identifier*
  (e.g. HORIZON-CL2-2026-01-DEMOCRACY-01 -> DEMOCRACY).
- Pillar 1: group by ERC / MSCA / INFRA.
- Pillar 3: group by EIC / EIE.

Destination names (destinationDescription) are *not* used, because they are missing for most calls.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from programme_groupers import (
    BaseProgrammeGrouper,
    SingletonMergeConfig,
    get_programme_grouper,
    normalize_space,
)


def merge_singletons_if_enabled(grouped: Dict[str, List[Dict[str, Any]]], grouper: BaseProgrammeGrouper) -> None:
    cfg: SingletonMergeConfig = grouper.merge_config()
    if not cfg.enabled or not grouped:
        return

    reps: Dict[str, set[str]] = {}
    for k, lst in grouped.items():
        if not isinstance(lst, list) or len(lst) < 2:
            continue
        agg: set[str] = set()
        for it in lst[:5]:
            agg |= grouper.tokenize_for_similarity(it.get("topic_title") or "", cfg.stopwords)
            agg |= grouper.tokenize_for_similarity(it.get("call_title") or "", cfg.stopwords)
        reps[k] = agg

    singleton_keys = [k for k, lst in grouped.items() if isinstance(lst, list) and len(lst) == 1]
    for sk in singleton_keys:
        single_item = grouped[sk][0]

        stoks = set()
        stoks |= grouper.tokenize_for_similarity(single_item.get("topic_title") or "", cfg.stopwords)
        stoks |= grouper.tokenize_for_similarity(single_item.get("call_title") or "", cfg.stopwords)

        best_k: Optional[str] = None
        best_score: float = 0.0

        for k, ktoks in reps.items():
            score = grouper.similarity(stoks, ktoks)
            if score > best_score:
                best_score = score
                best_k = k

        if best_k is not None and best_score >= float(cfg.sim_threshold):
            grouped[best_k].append(single_item)
        else:
            grouped[cfg.target_bucket].append(single_item)

        del grouped[sk]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input JSON list (Search API results OR fetched topic metadata list)")
    ap.add_argument("--out", required=True, help="Output JSON dict: group_value -> list[normalized items]")

    ap.add_argument("--programme", default="", help="Programme code selecting a grouping strategy (e.g. HORIZON, DEP).")

    # Backwards compatible for DefaultFieldGrouper
    ap.add_argument(
        "--group-by",
        default="callTitle",
        choices=["callTitle", "callIdentifier", "programmeDivision", "wpClass", "callFamily"],
    )
    ap.add_argument(
        "--fallback-group-by",
        default="callTitle",
        choices=["callTitle", "callIdentifier", "programmeDivision"],
    )

    ap.add_argument("--drop-unknown", action="store_true", help="Drop items that have no grouping value")
    ap.add_argument("--unknown-key", default="_unknown_group", help="Key to use when grouping value is missing")

    ap.add_argument("--merge-singletons", action="store_true")

    # NEW for HORIZON destination injection
    ap.add_argument("--cl3-map", default="", help="Path to horizon_CL3_destinations.json")
    ap.add_argument("--missions-map", default="", help="Path to horizon_missions_destinations.json")
    ap.add_argument("--cl1-map", default="", help="Optional: CL1 destination map json")

    args = ap.parse_args()

    grouper = get_programme_grouper(
        args.programme,
        group_by=args.group_by,
        fallback_group_by=args.fallback_group_by,
        cl3_map_path=args.cl3_map,
        missions_map_path=args.missions_map,
        cl1_map_path=args.cl1_map,
    )

    src = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if not isinstance(src, list):
        raise SystemExit("ERROR: expected input to be a JSON list of objects")

    seen: set[str] = set()
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for res in src:
        if not isinstance(res, dict):
            continue

        norm = grouper.normalize_one(res)
        ukey = grouper.unique_record_key(res)
        norm["unique_key"] = ukey

        if not (norm.get("identifier") or "").strip():
            continue

        if ukey in seen:
            continue
        seen.add(ukey)

        group_val = normalize_space(grouper.group_value(res))
        if not group_val:
            group_val = normalize_space(grouper.fallback_group_value(res))

        if not group_val:
            if args.drop_unknown:
                continue
            group_val = args.unknown_key

        norm["programme"] = (args.programme or grouper.programme_code or "").strip()
        norm["group_value"] = group_val

        grouped[group_val].append(norm)

    if args.merge_singletons and grouper.merge_config().enabled:
        merge_singletons_if_enabled(grouped, grouper)

    for k in list(grouped.keys()):
        grouped[k].sort(key=lambda x: ((x.get("identifier") or ""), (x.get("unique_key") or "")))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(dict(grouped), indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"OK: wrote {out_path} with {len(grouped)} groups and {len(seen)} unique calls")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

