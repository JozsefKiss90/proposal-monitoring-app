import json
import math
import time
import requests
from typing import Dict, Any, List, Set, Tuple

BASE_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"

def _multipart_json(name: str, obj: Any):
    # Mimic the portal: filename="blob", Content-Type: application/json
    return (f"{name}.json", json.dumps(obj), "application/json")

def fetch_framework_programme_calls(
    framework_programme_id: str,
    *,
    statuses=("31094501", "31094502"),  # forthcoming, open
    types=("1", "2", "8"),
    page_size=100,
    delay=0.2,
    max_pages_hard=5000,  # safety
) -> List[Dict[str, Any]]:

    query = {
        "bool": {
            "must": [
                {"terms": {"type": list(types)}},
                {"terms": {"status": list(statuses)}},
                {"terms": {"frameworkProgramme": [str(framework_programme_id)]}},
            ]
        }
    }

    sort = {"order": "DESC", "field": "startDate"}
    languages = ["en"]

    seen_keys: Set[Tuple[str, str, str, str]] = set()
    all_results: List[Dict[str, Any]] = []

    # First request: page 1, to learn totalResults (if present)
    page_number = 1
    params = {
        "apiKey": "SEDIA",
        "text": "***",
        "pageSize": page_size,
        "pageNumber": page_number,
    }

    files = {
        "query": _multipart_json("query", query),
        "sort": _multipart_json("sort", sort),
        "languages": _multipart_json("languages", languages),
    }

    r = requests.post(BASE_URL, params=params, files=files, timeout=60)
    r.raise_for_status()
    data = r.json()

    total_results = data.get("totalResults")
    results = data.get("results", []) or []
    if not results:
        return []

    # compute planned pages if totalResults available
    if isinstance(total_results, int) and total_results >= 0:
        planned_pages = max(1, math.ceil(total_results / page_size))
    else:
        planned_pages = None

    def add_results(batch: List[Dict[str, Any]]):
        for item in batch:
            # Robust dedupe key: favor callccm2Id when present
            meta = item.get("metadata") or {}
            callccm2 = ""
            if isinstance(meta.get("callccm2Id"), list) and meta["callccm2Id"]:
                callccm2 = str(meta["callccm2Id"][0])
            identifier = ""
            if isinstance(meta.get("identifier"), list) and meta["identifier"]:
                identifier = str(meta["identifier"][0])
            reference = ""
            if isinstance(meta.get("reference"), list) and meta["reference"]:
                reference = str(meta["reference"][0])

            key = (str(item.get("type", "")), callccm2, identifier, reference)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            all_results.append(item)

    add_results(results)

    # Fetch remaining pages
    if planned_pages is None:
        # Unknown total: keep going until empty
        page_iter_limit = max_pages_hard
    else:
        page_iter_limit = min(planned_pages, max_pages_hard)

    for page_number in range(2, page_iter_limit + 1):
        params["pageNumber"] = page_number
        r = requests.post(BASE_URL, params=params, files=files, timeout=60)
        r.raise_for_status()
        data = r.json()
        batch = data.get("results", []) or []
        if not batch:
            # empty page -> stop early (handles fluctuating totals)
            break
        add_results(batch)
        time.sleep(delay)

    return all_results
