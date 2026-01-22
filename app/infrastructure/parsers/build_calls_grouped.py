#!/usr/bin/env python3
"""
build_calls_grouped_patched.py

Group raw Search API results (list) into a dict:
  group_value -> list[normalized items]

Key fixes for DEP/DIGITAL:
- Do NOT dedupe by `metadata.identifier` alone (only ~37 unique in your DEP payload).
- Use a robust per-record uniqueness key: (type, identifier, callccm2Id, language).
- Optionally merge singleton groups into the closest larger group (simple token Jaccard).
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def safe_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    return str(x)


def first(x: Any, default: Any = "") -> Any:
    if isinstance(x, list):
        return x[0] if x else default
    return x if x is not None else default


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def parse_date(s: str) -> Optional[str]:
    s = safe_str(s).strip()
    if not s:
        return None
    try:
        s2 = s.replace("Z", "+0000")
        s2 = re.sub(r"([+-]\d{2}):(\d{2})$", r"\1\2", s2)
        dt = datetime.fromisoformat(s2.replace("+0000", "+00:00"))
        return dt.isoformat()
    except Exception:
        return s


def extract_budget(meta: Dict[str, Any]) -> Tuple[float, float, float, Any]:
    def _f(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    min_c = _f(first(meta.get("minContribution"), 0.0))
    max_c = _f(first(meta.get("maxContribution"), 0.0))
    b = first(meta.get("budget"), None)
    budget = _f(b) if b is not None else max_c
    n_projects = first(meta.get("expectedGrants"), None)
    return min_c, max_c, budget, n_projects


def unique_record_key(result: Dict[str, Any]) -> str:
    """DEP-safe record identity key."""
    meta = result.get("metadata") or {}
    typ = safe_str(first(meta.get("type"), result.get("type", ""))).strip()

    ident = safe_str(first(meta.get("identifier"), "")).strip()
    callccm2 = safe_str(first(meta.get("callccm2Id"), "")).strip()
    lang = safe_str(first(meta.get("language"), result.get("language", ""))).strip() or "und"

    return "|".join([typ, ident, callccm2, lang])


def normalize_one(result: Dict[str, Any]) -> Dict[str, Any]:
    meta = result.get("metadata") or {}

    identifier = safe_str(first(meta.get("identifier"), "")).strip()
    url = safe_str(first(meta.get("url"), "")).strip()

    topic_title = safe_str(first(meta.get("title"), "")).strip()
    call_title_section = safe_str(first(meta.get("callTitle"), "")).strip()

    opening = parse_date(first(meta.get("openingDate"), "")) or ""
    deadline = parse_date(first(meta.get("deadlineDate"), "")) or ""
    deadline_model = safe_str(first(meta.get("deadlineModel"), ""))

    expected_outcome = safe_str(first(meta.get("expectedOutcome"), ""))
    scope = safe_str(first(meta.get("scope"), ""))

    admissibility = safe_str(first(meta.get("admissibilityConditions"), ""))
    eligibility = safe_str(first(meta.get("eligibilityConditions"), ""))
    procedure = safe_str(first(meta.get("submissionProcedure"), ""))

    toa = safe_str(first(meta.get("typesOfAction"), ""))

    min_c, max_c, budget, n_projects = extract_budget(meta)

    tags = meta.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    keywords = meta.get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    return {
        "identifier": identifier,
        "url": url,
        "topic_title": topic_title,
        "call_title": topic_title,
        "call_section_title": call_title_section,
        "opening_date": opening,
        "deadline": deadline,
        "deadline_model": deadline_model,
        "status": safe_str(first(meta.get("status"), "")),
        "type_of_action": toa,
        "expected_outcome": expected_outcome,
        "scope": scope,
        "admissibility_conditions": admissibility,
        "eligibility_conditions": eligibility,
        "procedure": procedure,
        "min_contribution": min_c,
        "max_contribution": max_c,
        "indicative_budget": budget,
        "indicative_number_of_projects": n_projects,
        "tags": tags,
        "keywords": keywords,
        "raw": result,
    }


def derive_call_family_from_identifier(identifier: str) -> str:
    ident = normalize_space(identifier or "")
    if not ident:
        return ""
    parts = [p for p in ident.split("-") if p]
    if len(parts) < 2:
        return ident
    if len(parts) >= 4 and parts[0] == "DIGITAL" and parts[1].isdigit():
        return "-".join(parts[:4])
    if len(parts) >= 4 and parts[0] == "DIGITAL" and parts[1] in {"ECCC", "JU"}:
        return "-".join(parts[:4])
    if len(parts) >= 4:
        return "-".join(parts[:4])
    return ident


def derive_call_family(meta_wrapper: dict) -> str:
    meta = (meta_wrapper.get("metadata") or {})
    ci = safe_str(first(meta.get("callIdentifier"), "")).strip()
    if ci:
        return ci
    ident = safe_str(first(meta.get("identifier"), "")).strip()
    if ident:
        return derive_call_family_from_identifier(ident)
    return safe_str(first(meta.get("callTitle"), "")).strip()


def get_group_value(meta_wrapper: Dict[str, Any], group_by: str) -> str:
    meta = meta_wrapper.get("metadata") or {}
    if group_by == "callTitle":
        return safe_str(first(meta.get("callTitle"), ""))
    if group_by == "callIdentifier":
        return safe_str(first(meta.get("callIdentifier"), ""))
    if group_by == "programmeDivision":
        pd = meta.get("programmeDivision") or []
        if isinstance(pd, list) and pd:
            return ",".join([safe_str(x) for x in pd if safe_str(x)])
        return ""
    if group_by == "wpClass":
        ci = safe_str(first(meta.get("callIdentifier"), "")).strip()
        return ci or safe_str(first(meta.get("callTitle"), ""))
    if group_by == "callFamily":
        return derive_call_family(meta_wrapper)
    if group_by == "destinationDescription":
        return safe_str(first(meta.get("destinationDescription"), ""))
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--group-by",
        default="destinationDescription",
        choices=["destinationDescription","callTitle","callIdentifier","programmeDivision","wpClass","callFamily"],
    )
    ap.add_argument(
        "--fallback-group-by",
        default="callTitle",
        choices=["callTitle","callIdentifier","programmeDivision"],
    )
    ap.add_argument("--drop-unknown", action="store_true")
    ap.add_argument("--unknown-key", default="_unknown_group")
    ap.add_argument("--merge-singletons", action="store_true")
    ap.add_argument("--singletons-key", default="_other")
    ap.add_argument("--singleton-sim-threshold", type=float, default=0.28)
    args = ap.parse_args()

    src = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if not isinstance(src, list):
        raise SystemExit("ERROR: expected input to be a JSON list")

    seen: set[str] = set()
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for res in src:
        if not isinstance(res, dict):
            continue
        norm = normalize_one(res)
        ukey = unique_record_key(res)
        norm["unique_key"] = ukey

        if not norm.get("identifier"):
            continue
        if ukey in seen:
            continue
        seen.add(ukey)

        group_val = normalize_space(get_group_value(res, args.group_by))
        if not group_val:
            group_val = normalize_space(get_group_value(res, args.fallback_group_by))
        if not group_val:
            if args.drop_unknown:
                continue
            group_val = args.unknown_key

        norm["group_key"] = args.group_by
        norm["group_value"] = group_val
        grouped[group_val].append(norm)

    if args.merge_singletons and grouped:
        def _tokens(s: str) -> set[str]:
            s = normalize_space(s or "").lower()
            s = re.sub(r"[^a-z0-9]+", " ", s)
            toks = [t for t in s.split() if len(t) >= 3]
            stop = {"call","calls","topic","topics","programme","program","action","actions","open","forthcoming"}
            return {t for t in toks if t not in stop}

        def _best_target(single_item: dict) -> Optional[str]:
            stoks = _tokens(single_item.get("topic_title") or "") | _tokens(single_item.get("call_title") or "")
            if not stoks:
                return None
            best_k = None
            best_score = 0.0
            for k, lst in grouped.items():
                if len(lst) < 2:
                    continue
                ctoks: set[str] = set()
                for it in lst[:5]:
                    ctoks |= _tokens(it.get("topic_title") or "") | _tokens(it.get("call_title") or "")
                if not ctoks:
                    continue
                inter = len(stoks & ctoks)
                union = len(stoks | ctoks)
                score = (inter / union) if union else 0.0
                if score > best_score:
                    best_score = score
                    best_k = k
            if best_k and best_score >= float(args.singleton_sim_threshold):
                return best_k
            return None

        singleton_keys = [k for k, lst in grouped.items() if isinstance(lst, list) and len(lst) == 1]
        for sk in singleton_keys:
            single_item = grouped[sk][0]
            target = _best_target(single_item)
            if target:
                grouped[target].append(single_item)
            else:
                grouped[args.singletons_key].append(single_item)
            del grouped[sk]

    for k in list(grouped.keys()):
        grouped[k].sort(key=lambda x: (x.get("identifier") or "", x.get("unique_key") or ""))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(dict(grouped), indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"OK: wrote {out_path} with {len(grouped)} groups and {len(seen)} unique calls")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
