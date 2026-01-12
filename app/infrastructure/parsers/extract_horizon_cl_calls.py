# extract_horizon_cl_calls.py
import json
import re
import argparse
from pathlib import Path
from typing import Any, List, Set, Iterable

def walk(obj: Any) -> Iterable[str]:
    """Yield all strings found anywhere in a nested JSON-like object."""
    if isinstance(obj, dict):
        for v in obj.values():
            yield from walk(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from walk(it)
    elif isinstance(obj, str):
        yield obj

def extract_horizon_cl_topic_ids(
    data: Any,
    years: Set[str],
    clusters: Set[str],
) -> List[str]:
    """
    Extract topic IDs like:
      HORIZON-CL2-2027-01-DEMOCRACY-06
      HORIZON-CL2-2025-02-TRANSFO-04-two-stage
    but NOT base call sections like:
      HORIZON-CL2-2027-01
    """
    # Require: HORIZON-CL{n}-{year}-{2digitcall}-<something>
    # Allow lowercase (two-stage) and mixed case.
    year_re = "|".join(sorted(map(re.escape, years)))
    cl_re = "|".join(sorted(map(re.escape, clusters)))
    topic_pat = re.compile(
        rf"^HORIZON-CL(?:{cl_re})-(?:{year_re})-\d{{2}}-[A-Za-z0-9][A-Za-z0-9-]*$"
    )

    out: Set[str] = set()
    for s in walk(data):
        if topic_pat.match(s.strip()):
            out.add(s.strip())
    return sorted(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to response_body_2026.json")
    ap.add_argument("--out", required=True, help="Path to write extracted topic IDs JSON")
    ap.add_argument("--years", nargs="+", default=["2026", "2027"], help="Years to include (default: 2026 2027)")
    ap.add_argument("--clusters", nargs="+", default=["1","2","3","4","5","6"], help="Cluster numbers (default: 1..6)")
    args = ap.parse_args()

    data = json.loads(Path(args.input).read_text(encoding="utf-8"))
    years = set(args.years)
    clusters = set(args.clusters)

    ids = extract_horizon_cl_topic_ids(data, years=years, clusters=clusters)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(ids, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"OK: extracted {len(ids)} topic IDs -> {out_path}")

if __name__ == "__main__":
    main()
