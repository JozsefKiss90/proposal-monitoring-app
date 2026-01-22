#!/usr/bin/env python3
"""
Fetch raw topic metadata from the European Commission Search API (SEDIA).

This script intentionally returns ONE canonical record per topic ID to avoid
duplicate entries (the Search API often returns the same topic in many languages).

Selection rules:
- Prefer result.language == "en"
- Else prefer any result with non-empty summary/content
- Else fallback to first result

Input:  JSON list of topic IDs
Output: JSON list of objects:
  {
    "identifier": "<topic id queried>",
    "title": "<result.title>",
    "summary": "<result.summary>",
    "url": "<result.url>",
    "language": "<result.language>",
    "raw": { ... result ... }
  }

Usage:
  python fetch_call_metadata.py --input <topic_ids.json> --out <fetched.json>

Optional:
  --limit N
  --delay 0.5
  --timeout 30
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

BASE_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
API_KEY = "SEDIA"

def post_search(topic_id: str, timeout: int = 30) -> Dict[str, Any]:
    params = {"apiKey": API_KEY, "text": f"\"{topic_id}\""}
    r = requests.post(BASE_URL, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def pick_best_result(results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not results:
        return None

    # Prefer English, then highest weight (if present)
    def score(r: Dict[str, Any]) -> float:
        lang = (r.get("language") or "").lower()
        w = r.get("weight") or 0.0
        has_summary = 1.0 if (r.get("summary") or r.get("content")) else 0.0
        return (1000.0 if lang == "en" else 0.0) + (10.0 * has_summary) + float(w or 0.0)

    return max(results, key=score)


def fetch_all(topic_ids: List[str], delay: float, timeout: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    n = len(topic_ids)

    for i, topic_id in enumerate(topic_ids, 1):
        try:
            data = post_search(topic_id, timeout=timeout)
            results = data.get("results", []) or []
            best = pick_best_result(results)

            if best:
                out.append(
                    {
                        "identifier": topic_id,
                        "title": best.get("title"),
                        "summary": best.get("summary"),
                        "url": best.get("url"),
                        "language": best.get("language"),
                        "raw": best,
                    }
                )
                print(f"[{i}/{n}] OK   {topic_id} (picked language={best.get('language')})")
            else:
                out.append({"identifier": topic_id, "title": None, "summary": None, "url": None, "language": None, "raw": None})
                print(f"[{i}/{n}] MISS {topic_id} (no results)")
        except Exception as e:
            out.append({"identifier": topic_id, "title": None, "summary": None, "url": None, "language": None, "raw": None, "error": str(e)})
            print(f"[{i}/{n}] FAIL {topic_id}: {e}")

        if delay > 0:
            time.sleep(delay)

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input JSON list of topic IDs")
    ap.add_argument("--out", required=True, help="Output JSON list of fetched metadata")
    ap.add_argument("--delay", type=float, default=0.5, help="Delay between requests (seconds)")
    ap.add_argument("--timeout", type=int, default=30, help="Request timeout (seconds)")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of topic IDs (0 = no limit)")
    args = ap.parse_args()

    topic_ids = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if not isinstance(topic_ids, list):
        raise SystemExit("ERROR: input must be a JSON list of topic IDs")

    if args.limit and args.limit > 0:
        topic_ids = topic_ids[: args.limit]

    results = fetch_all(topic_ids, delay=args.delay, timeout=args.timeout)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"OK: saved {len(results)} records -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
