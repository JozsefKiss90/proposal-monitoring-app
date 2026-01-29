#!/usr/bin/env python3
"""
split_calls_by_cluster.py

1) Update grouped calls keys from destination CODE -> destination TITLE
   using an index mapping file (destination_code -> destination_title),
   including merging alt_codes into their canonical destination_code.

2) Split the (updated) destinations into programme buckets, writing one file per programme
   (e.g. HORIZON-CL2.json, HORIZON-MISS.json), based on the index file's top-level wrappers.

Designed to be flexible: it works for any "grouped calls" JSON that is:
  { "<group_key>": [ { ...record... }, ... ], ... }
and any "index" JSON that is:
  { "<programme>": [ {"destination_code":..., "destination_title":..., "alt_codes":[...]?}, ...], ... }

Typical usage (your current case):
  python split_calls_by_cluster.py \
    --grouped app/data/horizon_calls_grouped.json \
    --index   app/data/horizon_destinations_index_by_programme.json \
    --out-updated app/data/horizon_calls_grouped.titled.json \
    --out-dir app/data/splits

Notes:
- The script preserves the full call records (it does NOT strip metadata).
- If a grouped key isn't found in the index (neither as destination_code nor alt_code),
  it is kept under an "_UNMAPPED" programme bucket and its key is left as-is.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# -------------------------
# Data structures
# -------------------------

@dataclass(frozen=True)
class DestInfo:
    programme: str
    canonical_code: str
    title: str


# -------------------------
# IO helpers
# -------------------------

def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


# -------------------------
# Index parsing (build lookup tables)
# -------------------------

def build_destination_index(index_obj: Any) -> Tuple[Dict[str, DestInfo], Dict[str, str], Dict[str, DestInfo]]:
    """
    Returns:
      code_to_info: canonical destination_code -> DestInfo
      alt_to_canonical: alt_code -> canonical destination_code
      anycode_to_info: (canonical + alt) -> DestInfo  (alt resolves to canonical's DestInfo)
    """
    if not isinstance(index_obj, dict):
        raise SystemExit("ERROR: index JSON must be a dict keyed by programme name -> list of destinations")

    code_to_info: Dict[str, DestInfo] = {}
    alt_to_canonical: Dict[str, str] = {}

    for programme, dest_list in index_obj.items():
        if not isinstance(dest_list, list):
            continue
        for d in dest_list:
            if not isinstance(d, dict):
                continue
            code = str(d.get("destination_code") or "").strip()
            title = str(d.get("destination_title") or "").strip()
            if not code or not title:
                continue
            info = DestInfo(programme=str(programme), canonical_code=code, title=title)
            code_to_info[code] = info

            alt_codes = d.get("alt_codes") or []
            if isinstance(alt_codes, list):
                for a in alt_codes:
                    a2 = str(a or "").strip()
                    if a2:
                        alt_to_canonical[a2] = code

    anycode_to_info: Dict[str, DestInfo] = dict(code_to_info)
    for alt, canon in alt_to_canonical.items():
        info = code_to_info.get(canon)
        if info:
            anycode_to_info[alt] = info

    return code_to_info, alt_to_canonical, anycode_to_info


# -------------------------
# Grouped calls update + merge
# -------------------------

def normalize_grouped_calls(grouped_obj: Any) -> Dict[str, List[Dict[str, Any]]]:
    if not isinstance(grouped_obj, dict):
        raise SystemExit("ERROR: grouped calls JSON must be a dict of {group_key: [records...]}")
    out: Dict[str, List[Dict[str, Any]]] = {}
    for k, v in grouped_obj.items():
        if isinstance(v, list):
            out[str(k)] = [x for x in v if isinstance(x, dict)]
        else:
            out[str(k)] = []
    return out


def merge_alt_groups(
    grouped: Dict[str, List[Dict[str, Any]]],
    anycode_to_info: Dict[str, DestInfo],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str]]:
    """
    Merge groups whose key is an alt_code into their canonical_code group.

    Returns:
      merged_by_code: dict keyed by canonical_code where possible (unmapped keys stay as-is)
      key_resolution: original_key -> effective_key (canonical_code if mapped else original)
    """
    merged: Dict[str, List[Dict[str, Any]]] = {}
    key_resolution: Dict[str, str] = {}

    for key, records in grouped.items():
        info = anycode_to_info.get(key)
        if info:
            effective = info.canonical_code
        else:
            effective = key

        key_resolution[key] = effective
        merged.setdefault(effective, []).extend(records)

    # Deterministic ordering within each bucket, if identifiers exist
    for k in list(merged.keys()):
        merged[k].sort(key=lambda r: (str(r.get("identifier") or ""), str(r.get("unique_key") or "")))

    return merged, key_resolution


def retitle_groups(
    merged_by_code: Dict[str, List[Dict[str, Any]]],
    code_to_info: Dict[str, DestInfo],
    keep_original_group_value: bool,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str]]:
    """
    Convert canonical_code keys -> destination_title keys.

    Returns:
      titled: dict keyed by destination_title where possible (unmapped codes stay as code)
      code_to_title_used: canonical_code -> title (only for mapped codes)
    """
    titled: Dict[str, List[Dict[str, Any]]] = {}
    code_to_title_used: Dict[str, str] = {}

    for code, records in merged_by_code.items():
        info = code_to_info.get(code)
        if info:
            title = info.title
            code_to_title_used[code] = title
            out_key = title
        else:
            out_key = code  # unmapped remains as-is

        # Optionally update each record's group_value to the title
        if info and not keep_original_group_value:
            for r in records:
                if isinstance(r, dict):
                    r["group_value"] = info.title

        titled.setdefault(out_key, []).extend(records)

    # Deterministic ordering of destination keys
    titled_sorted = {k: titled[k] for k in sorted(titled.keys(), key=lambda s: s.lower())}
    return titled_sorted, code_to_title_used


# -------------------------
# Splitting into programme files
# -------------------------

def split_into_programmes(
    merged_by_code: Dict[str, List[Dict[str, Any]]],
    code_to_info: Dict[str, DestInfo],
    unmapped_programme: str,
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Returns:
      { programme_name: { destination_title_or_code: [records...] } }
    """
    out: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for code, records in merged_by_code.items():
        info = code_to_info.get(code)
        if info:
            programme = info.programme
            dest_key = info.title
        else:
            programme = unmapped_programme
            dest_key = code  # keep as-is

        out.setdefault(programme, {})
        out[programme].setdefault(dest_key, []).extend(records)

    # Sort destination keys inside each programme
    for prog in list(out.keys()):
        out[prog] = {k: out[prog][k] for k in sorted(out[prog].keys(), key=lambda s: s.lower())}

    return out


# -------------------------
# Main
# -------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--grouped", required=True, help="Input grouped calls JSON (e.g. horizon_calls_grouped.json)")
    ap.add_argument("--index", required=True, help="Index JSON (e.g. horizon_destinations_index_by_programme.json)")
    ap.add_argument(
        "--out-updated",
        required=True,
        help="Output path for updated grouped calls (destination titles as keys)",
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        help="Directory to write per-programme split files (e.g. HORIZON-CL2.json)",
    )
    ap.add_argument(
        "--unmapped-programme",
        default="_UNMAPPED",
        help="Programme bucket name for destination codes not found in the index",
    )
    ap.add_argument(
        "--keep-original-group-value",
        action="store_true",
        help="Do NOT overwrite each record's group_value with destination_title (only rename top-level keys).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print a report, but do not write output files.",
    )

    args = ap.parse_args()

    grouped_path = Path(args.grouped)
    index_path = Path(args.index)
    out_updated_path = Path(args.out_updated)
    out_dir = Path(args.out_dir)

    grouped_obj = read_json(grouped_path)
    index_obj = read_json(index_path)

    grouped = normalize_grouped_calls(grouped_obj)
    code_to_info, alt_to_canonical, anycode_to_info = build_destination_index(index_obj)

    # 1) Merge alt groups into canonical codes
    merged_by_code, key_resolution = merge_alt_groups(grouped, anycode_to_info)

    # 2) Retitle canonical-code groups into destination-title groups (for updated grouped file)
    titled_grouped, code_to_title_used = retitle_groups(
        merged_by_code,
        code_to_info,
        keep_original_group_value=bool(args.keep_original_group_value),
    )

    # 3) Split into programme files
    split = split_into_programmes(
        merged_by_code,
        code_to_info,
        unmapped_programme=str(args.unmapped_programme),
    )

    # Report
    original_groups = len(grouped)
    merged_groups = len(merged_by_code)
    titled_groups = len(titled_grouped)

    # Unmapped (after merging)
    unmapped_codes = sorted([c for c in merged_by_code.keys() if c not in code_to_info])

    # Alt groups that were actually merged
    merged_alt_keys = sorted([k for k, eff in key_resolution.items() if k != eff])

    print("---- split_calls_by_cluster.py report ----")
    print(f"Input grouped keys:       {original_groups}")
    print(f"After alt-code merge:     {merged_groups}")
    print(f"After retitle (titles):   {titled_groups}")
    print(f"Index canonical codes:    {len(code_to_info)}")
    print(f"Index alt_codes:          {len(alt_to_canonical)}")
    print(f"Alt-groups merged:        {len(merged_alt_keys)}")
    if merged_alt_keys:
        print("  examples:", ", ".join(merged_alt_keys[:10]) + (" ..." if len(merged_alt_keys) > 10 else ""))

    print(f"Unmapped destination codes/groups: {len(unmapped_codes)}")
    if unmapped_codes:
        print("  examples:", ", ".join(unmapped_codes[:12]) + (" ..." if len(unmapped_codes) > 12 else ""))

    print("Programme outputs:")
    for prog in sorted(split.keys()):
        n_dest = len(split[prog])
        n_calls = sum(len(v) for v in split[prog].values())
        print(f"  - {prog}: {n_dest} destinations, {n_calls} calls")

    if args.dry_run:
        print("DRY RUN: no files written.")
        return 0

    # Write updated grouped
    write_json(out_updated_path, titled_grouped)

    # Write per-programme files
    out_dir.mkdir(parents=True, exist_ok=True)
    for prog, prog_obj in split.items():
        out_path = out_dir / f"{prog}.json"
        write_json(out_path, prog_obj)

    print(f"OK: wrote updated grouped -> {out_updated_path}")
    print(f"OK: wrote programme splits -> {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

