#!/usr/bin/env python3
"""
extract_horizon_cl_calls.py

Extract identifiers from a FACET response body JSON.

Default (cluster mode: no --pillar1/--pillar3):
- Writes Pillar 2 cluster + mission IDs to --out
  (CL2–CL6 topics, CL1 HLTH topics, Missions)
- Writes WIDERA IDs to a SEPARATE file (NOT Pillar 2)
  - --widera-out if provided
  - else auto-derived from --out: insert ".widera" before ".json"

Pillar 1:
- ERC / MSCA / INFRA IDs

Pillar 3:
- EIC / EIE IDs (EIT intentionally omitted)

Union option:
- --include-clusters unions cluster-mode IDs into pillar modes (still excluding WIDERA from main out)
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable, Set


def walk_strings(obj: Any) -> Iterable[str]:
    if isinstance(obj, dict):
        for v in obj.values():
            yield from walk_strings(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from walk_strings(it)
    elif isinstance(obj, str):
        yield obj


def load_destination_call_ids_map(path: str) -> dict[str, list[str]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        out: dict[str, list[str]] = {}
        for k, v in data.items():
            if isinstance(v, list):
                out[str(k)] = [str(x) for x in v if isinstance(x, str)]
        return out
    if isinstance(data, list):
        out: dict[str, list[str]] = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            dest = str(row.get("destination") or "").strip()
            call_ids = row.get("call_ids") or []
            if dest and isinstance(call_ids, list):
                out[dest] = [str(x) for x in call_ids if isinstance(x, str)]
        return out
    return {}


# -------------------------
# Cluster extraction (Pillar 2)
# -------------------------

def extract_topic_ids_from_facet(data: Any, years: Set[str], clusters: Set[str]) -> Set[str]:
    """
    Extract CL2–CL6 topic IDs from FACET:
      HORIZON-CL{2..6}-YYYY-NN-...
    """
    clusters_wo_1 = sorted({c for c in clusters if c != "1"})
    if not clusters_wo_1:
        return set()

    year_re = "|".join(sorted(map(re.escape, years)))
    cl_re = "|".join(sorted(map(re.escape, clusters_wo_1)))
    pat = re.compile(rf"^HORIZON-CL(?:{cl_re})-(?:{year_re})-\d{{2}}-[A-Za-z0-9][A-Za-z0-9-]*$")

    out: Set[str] = set()
    for s in walk_strings(data):
        s2 = s.strip()
        if pat.match(s2):
            out.add(s2)
    return out


def extract_cl1_ids_from_facet(data: Any, years: Set[str]) -> Set[str]:
    """
    Extract CL1 (Health) topic IDs from FACET:
      HORIZON-HLTH-YYYY-NN-...
    """
    year_re = "|".join(sorted(map(re.escape, years)))
    pat = re.compile(rf"^HORIZON-HLTH-(?:{year_re})-\d{{2}}-[A-Za-z0-9][A-Za-z0-9-]*$")

    out: Set[str] = set()
    for s in walk_strings(data):
        s2 = s.strip()
        if pat.match(s2):
            out.add(s2)
    return out


def extract_cl1_ids_from_map(map_path: str, years: Set[str]) -> Set[str]:
    if not map_path:
        return set()

    dest_map = load_destination_call_ids_map(map_path)
    out: Set[str] = set()
    pat = re.compile(r"^HORIZON-HLTH-(\d{4})-\d{2}-[A-Za-z0-9][A-Za-z0-9-]*$")

    for _, call_ids in dest_map.items():
        for cid in call_ids:
            c = (cid or "").strip()
            if not c:
                continue
            m = pat.match(c)
            if not m:
                continue
            year = m.group(1)
            if year in years:
                out.add(c)
    return out


def extract_missions_ids_from_facet(data: Any, years: Set[str]) -> Set[str]:
    """
    Extract Horizon Missions IDs from FACET:
      HORIZON-MISS-YYYY-...
    """
    year_re = "|".join(sorted(map(re.escape, years)))
    pat = re.compile(rf"^HORIZON-MISS-(?:{year_re})-[A-Za-z0-9][A-Za-z0-9-]*$")

    out: Set[str] = set()
    for s in walk_strings(data):
        s2 = s.strip()
        if pat.match(s2):
            out.add(s2)
    return out


# -------------------------
# WIDERA (standalone HE part; NOT pillar 2)
# -------------------------

def extract_widera_ids_from_facet(data: Any, years: Set[str]) -> Set[str]:
    """
    Extract WIDERA IDs from FACET:
      HORIZON-WIDERA-YYYY-...
    """
    year_re = "|".join(sorted(map(re.escape, years)))
    pat = re.compile(rf"^HORIZON-WIDERA-(?:{year_re})-[A-Za-z0-9][A-Za-z0-9-]*$")

    out: Set[str] = set()
    for s in walk_strings(data):
        s2 = s.strip()
        if pat.match(s2):
            out.add(s2)
    return out


# -------------------------
# Pillar 1
# -------------------------

def extract_pillar1_ids_from_facet(data: Any, years: Set[str]) -> Set[str]:
    year_re = "|".join(sorted(map(re.escape, years)))
    erc_pat = re.compile(rf"^(?:HORIZON-ERC|ERC)-(?:{year_re})-[A-Za-z0-9][A-Za-z0-9-]*$")
    msca_pat = re.compile(rf"^HORIZON-MSCA-(?:{year_re})-[A-Za-z0-9][A-Za-z0-9-]*$")
    infra_pat = re.compile(rf"^HORIZON-INFRA-(?:{year_re})-[A-Za-z0-9][A-Za-z0-9-]*$")

    out: Set[str] = set()
    for s in walk_strings(data):
        s2 = s.strip()
        if not s2:
            continue
        if erc_pat.match(s2) or msca_pat.match(s2) or infra_pat.match(s2):
            out.add(s2)
    return out


# -------------------------
# Pillar 3 (EIC/EIE only)
# -------------------------

def extract_pillar3_ids_from_facet(data: Any, years: Set[str], include_eic: bool, include_eie: bool) -> Set[str]:
    year_re = "|".join(sorted(map(re.escape, years)))
    eic_pat = re.compile(rf"^HORIZON-EIC-(?:{year_re})-[A-Za-z0-9][A-Za-z0-9-]*$")
    eie_pat = re.compile(rf"^HORIZON-EIE-(?:{year_re})-[A-Za-z0-9][A-Za-z0-9-]*$")

    out: Set[str] = set()
    for s in walk_strings(data):
        s2 = s.strip()
        if not s2:
            continue
        if include_eic and eic_pat.match(s2):
            out.add(s2)
            continue
        if include_eie and eie_pat.match(s2):
            out.add(s2)
            continue
    return out


def derive_widera_out_path(out_path: Path) -> Path:
    # Insert ".widera" before .json (or append if no suffix)
    if out_path.suffix.lower() == ".json":
        return out_path.with_name(out_path.stem + ".widera.json")
    return out_path.with_name(out_path.name + ".widera.json")


def write_ids(path: Path, ids: Set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(ids), indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="FACET response body JSON (e.g. response_body_2026.json)")
    ap.add_argument("--out", required=True, help="Main output JSON path (clusters/missions OR pillar output)")
    ap.add_argument("--years", nargs="+", default=["2026", "2027"])

    # NEW: separate WIDERA output
    ap.add_argument(
        "--widera-out",
        default="",
        help="Output JSON path for WIDERA IDs (standalone part of Horizon Europe; not pillar 2). "
             "If omitted, auto-derived from --out.",
    )

    # Clusters (Pillar 2)
    ap.add_argument("--clusters", nargs="+", default=["1", "2", "3", "4", "5", "6"])
    ap.add_argument("--cl1-map", default="", help="Optional: CL1 destination map json (union-inject HLTH IDs)")

    # Pillar 1
    ap.add_argument("--pillar1", action="store_true", help="Extract Pillar 1 identifiers (ERC, MSCA, INFRA).")

    # Pillar 3
    ap.add_argument("--pillar3", action="store_true", help="Extract Pillar 3 identifiers (EIC, EIE).")
    ap.add_argument("--eic", action="store_true", help="With --pillar3: include EIC IDs.")
    ap.add_argument("--eie", action="store_true", help="With --pillar3: include EIE IDs.")

    # Union option
    ap.add_argument("--include-clusters", action="store_true", help="With pillar modes, union-inject clusters/missions.")

    args = ap.parse_args()

    years: Set[str] = set(map(str, args.years))
    clusters: Set[str] = set(map(str, args.clusters))

    data = json.loads(Path(args.input).read_text(encoding="utf-8"))

    # Always extract WIDERA separately (standalone HE part)
    widera_ids = extract_widera_ids_from_facet(data, years=years)

    out_path = Path(args.out)
    widera_out_path = Path(args.widera_out) if args.widera_out else derive_widera_out_path(out_path)

    # Main ids (pillar2 clusters/missions or pillar1/pillar3 output)
    ids: Set[str] = set()

    def add_clusters_and_missions() -> None:
        nonlocal ids
        ids |= extract_topic_ids_from_facet(data, years=years, clusters=clusters)
        if "1" in clusters:
            ids |= extract_cl1_ids_from_facet(data, years=years)
            if args.cl1_map:
                ids |= extract_cl1_ids_from_map(args.cl1_map, years=years)
        ids |= extract_missions_ids_from_facet(data, years=years)
        # NOTE: WIDERA deliberately NOT included in ids

    if args.pillar1 or args.pillar3:
        if args.pillar1:
            ids |= extract_pillar1_ids_from_facet(data, years=years)

        if args.pillar3:
            include_eic = args.eic or (not args.eic and not args.eie)
            include_eie = args.eie or (not args.eic and not args.eie)
            ids |= extract_pillar3_ids_from_facet(data, years=years, include_eic=include_eic, include_eie=include_eie)

        if args.include_clusters:
            add_clusters_and_missions()
    else:
        # Default: cluster mode (pillar 2 clusters + missions)
        add_clusters_and_missions()

    # Write outputs
    write_ids(out_path, ids)
    print(f"OK: extracted {len(ids)} IDs -> {out_path} (NO WIDERA)")

    if widera_ids:
        write_ids(widera_out_path, widera_ids)
        print(f"OK: extracted {len(widera_ids)} WIDERA IDs -> {widera_out_path}")
    else:
        print("OK: no WIDERA IDs found for the selected years")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
