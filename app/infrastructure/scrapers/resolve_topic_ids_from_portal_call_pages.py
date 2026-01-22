#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup


TOPIC_DETAILS_RE = re.compile(r"/opportunities/data/topicDetails/([A-Za-z0-9][A-Za-z0-9\-_.]*)\.json")


@dataclass
class PortalResolution:
    portal_call_id: str
    portal_url: str
    topic_identifier: Optional[str] = None
    topic_details_url: Optional[str] = None
    note: Optional[str] = None


def resolve_one(portal_url: str, portal_call_id: str, timeout: int = 30) -> PortalResolution:
    r = requests.get(portal_url, timeout=timeout)
    r.raise_for_status()
    html = r.text

    # Some pages embed the topicDetails link in HTML; others in scripts.
    m = TOPIC_DETAILS_RE.search(html)
    if m:
        ident = m.group(1)
        full = "https://ec.europa.eu/info/funding-tenders/opportunities/data/topicDetails/" + ident + ".json"
        return PortalResolution(
            portal_call_id=portal_call_id,
            portal_url=portal_url,
            topic_identifier=ident,
            topic_details_url=full,
        )

    # Fallback: parse anchors if present
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        hm = TOPIC_DETAILS_RE.search(a["href"])
        if hm:
            ident = hm.group(1)
            full = "https://ec.europa.eu/info/funding-tenders/opportunities/data/topicDetails/" + ident + ".json"
            return PortalResolution(
                portal_call_id=portal_call_id,
                portal_url=portal_url,
                topic_identifier=ident,
                topic_details_url=full,
            )

    return PortalResolution(
        portal_call_id=portal_call_id,
        portal_url=portal_url,
        note="No topicDetails link found. Likely cascade funding or non-topic-backed record.",
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listing-json", required=True, help="Output from scrape_portal_calls_listing.py")
    ap.add_argument("--out", required=True, help="Output JSON of resolved topic identifiers")
    ap.add_argument(
        "--base",
        default="https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/competitive-calls-cs",
        help="Base URL prefix for portal detail pages",
    )
    ap.add_argument("--delay", type=float, default=0.5, help="Delay between requests (seconds)")
    args = ap.parse_args()

    listing = json.loads(Path(args.listing_json).read_text(encoding="utf-8"))
    out: List[PortalResolution] = []

    for i, row in enumerate(listing, 1):
        pid = str(row.get("portal_call_id") or "").strip()
        if not pid:
            continue

        url = f"{args.base}/{pid}"
        try:
            res = resolve_one(url, pid)
            out.append(res)
            print(f"[{i}/{len(listing)}] OK {pid} -> {res.topic_identifier or 'NO_TOPIC'}")
        except Exception as e:
            out.append(PortalResolution(portal_call_id=pid, portal_url=url, note=f"ERROR: {e}"))
            print(f"[{i}/{len(listing)}] ERROR {pid}: {e}")

        time.sleep(args.delay)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps([asdict(x) for x in out], indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"OK: wrote {len(out)} resolutions -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
