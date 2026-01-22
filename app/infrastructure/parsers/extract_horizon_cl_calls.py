#!/usr/bin/env python3
"""
Extract identifiers from a FACET API response body.

Preserves the original behaviour for Horizon Europe Clusters (CL1–CL6):
- CL2–CL6 topic IDs are extracted directly from the FACET JSON:
    HORIZON-CLx-YYYY-NN-...
- CL1 (Health) is injected from a mapping JSON because FACET may omit it:
    HORIZON-HLTH-YYYY-NN-...

Supports Pillar 1 IDs (ERC, MSCA, INFRA):
  --pillar1

Adds Pillar 3 IDs (EIC, EIE):
  --pillar3 [--eic] [--eie]
  If --pillar3 is set and neither --eic nor --eie is provided, BOTH are included by default.

EIT is intentionally omitted (per project decision: not open / not available in portal).

Union options:
- --include-clusters can be used with --pillar1 or --pillar3 to union pillar IDs + clusters.
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
    """
    Supports both structures:
    1) dict: { "Destination ...": ["CALL1", "CALL2"] }
    2) list: [ { "destination": "...", "call_ids": [...] }, ... ]
    Returns dict[dest] -> list[call_id]
    """
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
# Cluster extraction (original behaviour)
# -------------------------

def extract_topic_ids_from_facet(data: Any, years: Set[str], clusters: Set[str]) -> Set[str]:
    """
    Extract cluster topic IDs present in FACET:
      HORIZON-CL{cluster}-{year}-{2digitcall}-<topic...>
    """
    year_re = "|".join(sorted(map(re.escape, years)))
    cl_re = "|".join(sorted(map(re.escape, clusters)))

    pat = re.compile(rf"^HORIZON-CL(?:{cl_re})-(?:{year_re})-\d{{2}}-[A-Za-z0-9][A-Za-z0-9-]*$")

    out: Set[str] = set()
    for s in walk_strings(data):
        s2 = s.strip()
        if pat.match(s2):
            out.add(s2)
    return out


def extract_cl1_ids_from_map(map_path: str, years: Set[str]) -> Set[str]:
    """
    Inject CL1 IDs from map:
      HORIZON-HLTH-YYYY-NN-...
    """
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


# -------------------------
# Pillar 1 extraction
# -------------------------

def extract_pillar1_ids_from_facet(data: Any, years: Set[str]) -> Set[str]:
    """
    Extract Pillar 1 IDs from FACET:
      - ERC: (ERC-YYYY-...) or (HORIZON-ERC-YYYY-...)
      - MSCA: HORIZON-MSCA-YYYY-...
      - INFRA: HORIZON-INFRA-YYYY-...

    Patterns are intentionally permissive after the year to handle real ID shapes like:
      HORIZON-MSCA-2026-DN-01
      HORIZON-MSCA-2027-CITIZENS-01-01
      HORIZON-INFRA-2026-DEV-01-01
      HORIZON-INFRA-2026-TECH-01-02
    """
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
# Pillar 3 extraction (EIC, EIE; omit EIT)
# -------------------------

def extract_pillar3_ids_from_facet(
    data: Any,
    years: Set[str],
    include_eic: bool,
    include_eie: bool,
) -> Set[str]:
    """
    Extract Pillar 3 IDs from FACET:
      - EIC: HORIZON-EIC-YYYY-...
      - EIE: HORIZON-EIE-YYYY-...

    Note: EIT intentionally omitted.
    """
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


# -------------------------
# CLI
# -------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="FACET response body JSON (e.g. response_body_2026.json)")
    ap.add_argument("--out", required=True, help="Output JSON path (list of extracted IDs)")
    ap.add_argument("--years", nargs="+", default=["2026", "2027"])

    # Clusters (kept)
    ap.add_argument("--clusters", nargs="+", default=["1", "2", "3", "4", "5", "6"])
    ap.add_argument(
        "--cl1-map",
        default="",
        help="Path to c1_destinations_call_ids.json (inject CL1 HLTH IDs when FACET omits them)",
    )

    # Pillar 1
    ap.add_argument(
        "--pillar1",
        action="store_true",
        help="Extract Pillar 1 identifiers (ERC, MSCA, INFRA).",
    )

    # Pillar 3
    ap.add_argument(
        "--pillar3",
        action="store_true",
        help="Extract Pillar 3 identifiers (EIC, EIE).",
    )
    ap.add_argument(
        "--eic",
        action="store_true",
        help="With --pillar3: include EIC identifiers (HORIZON-EIC-YYYY-...).",
    )
    ap.add_argument(
        "--eie",
        action="store_true",
        help="With --pillar3: include EIE identifiers (HORIZON-EIE-YYYY-...).",
    )

    # Union option
    ap.add_argument(
        "--include-clusters",
        action="store_true",
        help="When used with --pillar1 or --pillar3, also include clusters (union).",
    )

    args = ap.parse_args()

    years: Set[str] = set(map(str, args.years))
    clusters: Set[str] = set(map(str, args.clusters))

    data = json.loads(Path(args.input).read_text(encoding="utf-8"))

    ids: Set[str] = set()

    # Determine mode
    if args.pillar1 or args.pillar3:
        if args.pillar1:
            ids |= extract_pillar1_ids_from_facet(data, years=years)

        if args.pillar3:
            # Default behaviour: if user didn't specify any subcomponent flags, include both.
            include_eic = args.eic or (not args.eic and not args.eie)
            include_eie = args.eie or (not args.eic and not args.eie)
            ids |= extract_pillar3_ids_from_facet(
                data,
                years=years,
                include_eic=include_eic,
                include_eie=include_eie,
            )

        if args.include_clusters:
            ids |= extract_topic_ids_from_facet(data, years=years, clusters=clusters)

            if "1" in clusters:
                if args.cl1_map:
                    ids |= extract_cl1_ids_from_map(args.cl1_map, years=years)
                else:
                    print("[WARN] CL1 requested but --cl1-map not provided; CL1 (HORIZON-HLTH-...) may be missing.")
    else:
        # Default cluster-only mode (original behaviour)
        ids |= extract_topic_ids_from_facet(data, years=years, clusters=clusters)

        if "1" in clusters:
            if args.cl1_map:
                ids |= extract_cl1_ids_from_map(args.cl1_map, years=years)
            else:
                print("[WARN] CL1 requested but --cl1-map not provided; CL1 (HORIZON-HLTH-...) may be missing.")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(sorted(ids), indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"OK: extracted {len(ids)} IDs -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

