#!/usr/bin/env python3
"""
Fetch call metadata from the EU Search API (POST) and extract key fields,
including Expected Outcome and Scope from `metadata.descriptionByte`.

Usage:
  python fetch_api.py HORIZON-CL2-2025-01-DEMOCRACY-01
  python fetch_api.py --api-key SEDIA HORIZON-CL2-2025-01-DEMOCRACY-01
"""

import json
import sys
import argparse
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# --- API config ---
SEARCH_API_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
DEFAULT_API_KEY = "SEDIA"
REQUEST_TIMEOUT = 25


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
                bullet = "- " + li.get_text(" ", strip=True)
                parts.append(bullet)
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

    # Each section starts with: <span class="topicdescriptionkind">Expected Outcome:</span>
    for heading in soup.find_all("span", class_="topicdescriptionkind"):
        key = normalize_heading(heading.get_text(strip=True))
        section_nodes = []
        # collect siblings until the next heading span
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
    status_code = first(meta.get("status"))  # often a numeric code

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

    # Destination, types of action
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


def fetch_from_api(call_identifier: str, api_key: str = DEFAULT_API_KEY) -> Dict[str, Any]:
    """
    POST to the EU Search API with the quoted identifier in the URL query string,
    as requested. (Body is empty; server reads params from the URL.)
    """
    # Build URL with required quoted text
    params = {
        "apiKey": api_key,
        "text": f"\"{call_identifier}\"",
    }
    # Let requests encode params; this yields the same URL with %22-encoded quotes.
    # POST is required by the endpoint (GET may return 405).
    headers = {
        "Accept": "application/json",
    }
    r = requests.post(SEARCH_API_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    payload = r.json()

    # The API returns a list under "results"
    results = payload.get("results") or []
    picked = pick_best_result(results, call_identifier)
    if not picked:
        return {"error": f"No results for {call_identifier}", "query": params}

    meta = picked.get("metadata") or {}
    return extract_from_metadata(meta, call_identifier)


# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch HORIZON call metadata from the EU Search API (POST) and extract key fields."
    )
    parser.add_argument("call_identifier", help='e.g. HORIZON-CL2-2025-01-DEMOCRACY-01')
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="API key for the Search API (default: SEDIA)")
    args = parser.parse_args()

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
