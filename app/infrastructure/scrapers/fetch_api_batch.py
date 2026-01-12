#!/usr/bin/env python3
"""
Batch-fetch call/topic metadata from the EU Search API (POST), extract key fields
(including Expected Outcome & Scope from metadata.descriptionByte), group by
'destination', and write the grouped results to JSON.

Examples:
  # Batch mode (recommended):
  python fetch_api.py --input-file horizon_cl_all_2025_calls.json --out grouped_2025_calls_by_destination.json

  # Single ID mode:
  python fetch_api.py HORIZON-CL2-2025-01-DEMOCRACY-01
"""

import json
import sys
import argparse
from typing import Any, Dict, List, Optional, Iterable
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import requests
from bs4 import BeautifulSoup

# --- API config ---
SEARCH_API_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
DEFAULT_API_KEY = "SEDIA"
REQUEST_TIMEOUT = 25
CONCURRENCY = 6
RETRY_ATTEMPTS = 2
RETRY_SLEEP = 0.8  # seconds


# ---------- HTML helpers for `descriptionByte` ----------

def normalize_heading(text: str) -> str:
    if not text:
        return ""
    return text.replace(":", "").strip()

def html_section_to_text(elements) -> str:
    """Turn a sequence of <p>, <ul>/<li> etc. into a clean multiline string."""
    parts: List[str] = []
    for el in elements:
        name = getattr(el, "name", None)
        if name == "p":
            txt = el.get_text(" ", strip=True)
            if txt:
                parts.append(txt)
        elif name in ("ul", "ol"):
            for li in el.find_all("li", recursive=False):
                parts.append("- " + li.get_text(" ", strip=True))
        elif name == "li":
            parts.append("- " + el.get_text(" ", strip=True))
    return "\n".join(parts).strip()

def extract_description_fields(html_content: str) -> Dict[str, str]:
    """
    Parse the HTML from descriptionByte and return sections like
    'Expected Outcome' and 'Scope'. Handles paragraphs and bullet lists.
    """
    soup = BeautifulSoup(html_content or "", "html.parser")
    results: Dict[str, str] = {}
    for heading in soup.find_all("span", class_="topicdescriptionkind"):
        key = normalize_heading(heading.get_text(strip=True))
        section_nodes = []
        for sib in heading.next_siblings:
            if getattr(sib, "name", None) == "span" and "topicdescriptionkind" in (sib.get("class") or []):
                break
            if getattr(sib, "name", None) in ("p", "ul", "ol", "li"):
                section_nodes.append(sib)
        results[key] = html_section_to_text(section_nodes)
    return results


# ---------- JSON helpers ----------

def first(v: Any, default: Optional[Any] = None) -> Any:
    if isinstance(v, list) and v:
        return v[0]
    return v if v is not None else default

def try_json_loads(s: Optional[str]) -> Optional[Any]:
    if not s or not isinstance(s, str):
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


# ---------- Core extraction ----------

def pick_best_result(results: List[Dict[str, Any]], call_identifier: str) -> Optional[Dict[str, Any]]:
    """
    From the Search API's results, return the one whose metadata.identifier
    exactly matches the requested call identifier (preferred),
    otherwise the first result that contains it, otherwise the first result.
    """
    exact = None
    contains = None
    for r in results:
        meta = r.get("metadata", {})
        identifiers = meta.get("identifier") or []
        if call_identifier in identifiers:
            exact = r
            break
        if any(call_identifier in str(x) for x in identifiers):
            contains = contains or r
    return exact or contains or (results[0] if results else None)


def extract_from_metadata(meta: Dict[str, Any], call_identifier: Optional[str]) -> Dict[str, Any]:
    # Identifier
    identifiers = meta.get("identifier") or []
    identifier = call_identifier or first(identifiers)

    # Titles & links
    topic_title = first(meta.get("title"))
    call_title = first(meta.get("callTitle"))
    url = first(meta.get("url"))

    # Dates, status, model
    start_date = first(meta.get("startDate"))
    deadline_date = first(meta.get("deadlineDate"))
    deadline_model = first(meta.get("deadlineModel"))
    status_code = first(meta.get("status"))  # often numeric code

    # Parse richer info from "actions" (stringified JSON array)
    actions = try_json_loads(first(meta.get("actions")))
    status_text = None
    submission_model_text = None
    planned_opening_date = None
    action_deadlines = None
    if isinstance(actions, list) and actions:
        a0 = actions[0]
        status_text = (a0.get("status") or {}).get("description") or None
        submission_model_text = (a0.get("submissionProcedure") or {}).get("description") or None
        planned_opening_date = a0.get("plannedOpeningDate") or None
        action_deadlines = a0.get("deadlineDates") or None

    # Prefer richer fields from actions when present
    start_date = planned_opening_date or start_date
    deadline_model = submission_model_text or deadline_model
    if isinstance(action_deadlines, list) and action_deadlines:
        deadline_date = action_deadlines[0]

    # Destination & type of action (used for grouping)
    destination = first(meta.get("destinationDescription"))
    types_of_action = first(meta.get("typesOfAction"))

    # Budget details from "budgetOverview" (stringified JSON)
    budget_details = None
    budget = try_json_loads(first(meta.get("budgetOverview")))
    if isinstance(budget, dict):
        for actions_list in budget.get("budgetTopicActionMap", {}).values():
            for action in actions_list:
                action_text = action.get("action", "")
                if identifier and identifier in action_text:
                    budget_details = action
                    break
            if budget_details:
                break

    # Description sections (Expected Outcome & Scope)
    desc_html = first(meta.get("descriptionByte")) or ""
    sections = extract_description_fields(desc_html)

    return {
        "identifier": identifier,
        "topic_title": topic_title,
        "call_title": call_title,
        "url": url,
        "destination": destination,
        "type_of_action": types_of_action,
        "status": status_text or status_code,
        "start_date": start_date,
        "deadline_date": deadline_date,
        "deadline_model": deadline_model,
        "expected_outcome": sections.get("Expected Outcome", ""),
        "scope": sections.get("Scope", ""),
        "budget": budget_details,
        "tags": meta.get("tags") or [],
        "keywords": meta.get("keywords") or [],
    }


def _post_search(params: Dict[str, str]) -> Dict[str, Any]:
    headers = {"Accept": "application/json"}
    r = requests.post(SEARCH_API_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_from_api(call_identifier: str, api_key: str = DEFAULT_API_KEY) -> Dict[str, Any]:
    """
    POST to the EU Search API with the quoted identifier in the URL query string.
    Includes a fallback (no quotes) if the strict search returns nothing.
    """
    params = {"apiKey": api_key, "text": f"\"{call_identifier}\""}

    payload = _post_search(params)
    results = payload.get("results") or []
    if not results:
        # Fallback: try without quotes (useful for some IDs)
        time.sleep(0.2)
        payload = _post_search({"apiKey": api_key, "text": str(call_identifier)})
        results = payload.get("results") or []

    picked = pick_best_result(results, call_identifier)
    if not picked:
        return {"identifier": call_identifier, "error": f"No results for {call_identifier}"}

    meta = picked.get("metadata") or {}
    return extract_from_metadata(meta, call_identifier)


# ---------- Batch helpers ----------

def load_identifiers_from_file(path: str) -> List[str]:
    """
    Accepts a JSON file containing either:
      - an array of strings: ["HORIZON-CL2-...", "HORIZON-..."] or ["101235387", ...]
      - an array of objects with an 'identifier' field.
    De-duplicates while preserving order.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_ids: List[str] = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                raw_ids.append(item.strip())
            elif isinstance(item, dict) and "identifier" in item:
                raw_ids.append(str(item["identifier"]).strip())
            else:
                # ignore unknown item kinds
                pass
    else:
        raise ValueError("Input JSON must be an array of strings or objects with 'identifier'.")

    # De-dupe but keep order
    seen = set()
    ids: List[str] = []
    for x in raw_ids:
        if x and x not in seen:
            seen.add(x)
            ids.append(x)
    return ids


def fetch_with_retries(call_identifier: str, api_key: str) -> Dict[str, Any]:
    last_err: Optional[str] = None
    for attempt in range(1, RETRY_ATTEMPTS + 2):  # 1 try + RETRY_ATTEMPTS retries
        try:
            return fetch_from_api(call_identifier, api_key)
        except requests.HTTPError as e:
            last_err = f"HTTP {e.response.status_code if e.response else 'N/A'}: {e}"
        except requests.RequestException as e:
            last_err = f"Request failed: {e}"
        except Exception as e:
            last_err = f"Unexpected error: {e}"
        time.sleep(RETRY_SLEEP * attempt)
    return {"identifier": call_identifier, "error": last_err or "Unknown error"}


def batch_fetch_grouped(identifiers: Iterable[str], api_key: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch each identifier and group results by 'destination'.
    Unknown/empty destination goes under '_unknown_destination'.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        future_map = {ex.submit(fetch_with_retries, ident, api_key): ident for ident in identifiers}
        for fut in as_completed(future_map):
            data = fut.result()
            dest = (data.get("destination") or "_unknown_destination").strip() or "_unknown_destination"
            grouped[dest].append(data)

    # stable order by identifier within each destination
    for dest in grouped:
        grouped[dest].sort(key=lambda x: (x.get("identifier") or ""))

    return dict(grouped)


# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch HORIZON call/topic metadata from the EU Search API (POST), group by destination, and save to JSON."
    )
    parser.add_argument("call_identifier", nargs="?", help="Single identifier (e.g., HORIZON-CL2-2025-01-DEMOCRACY-01)")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="API key for the Search API (default: SEDIA)")
    parser.add_argument("--input-file", help="JSON file with an array of identifiers")
    parser.add_argument("--out", default="calls_grouped_by_destination.json", help="Output JSON file (batch mode)")
    args = parser.parse_args()

    # Batch mode
    if args.input_file:
        try:
            ids = load_identifiers_from_file(args.input_file)
            if not ids:
                print(json.dumps({"error": "No identifiers found in input file."}, ensure_ascii=False))
                sys.exit(1)

            grouped = batch_fetch_grouped(ids, args.api_key)
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(grouped, f, indent=2, ensure_ascii=False)
            print(json.dumps({"ok": True, "written": args.out, "destinations": list(grouped.keys())}, ensure_ascii=False))
        except Exception as e:
            print(json.dumps({"error": "Batch failed", "details": str(e)}, ensure_ascii=False))
            sys.exit(1)
        return

    # Single-ID mode (backwards compatible)
    if not args.call_identifier:
        parser.error("either --input-file must be provided for batch mode, or a single call_identifier for single mode")

    try:
        out = fetch_from_api(args.call_identifier, args.api_key)
        print(json.dumps(out, indent=2, ensure_ascii=False))
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "N/A"
        print(json.dumps({"error": f"HTTP {status}", "details": str(e)}, ensure_ascii=False))
        sys.exit(1)
    except requests.RequestException as e:
        print(json.dumps({"error": "Request failed", "details": str(e)}, ensure_ascii=False))
        sys.exit(1)
    except Exception as e:
        print(json.dumps({"error": "Unexpected error", "details": str(e)}, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()