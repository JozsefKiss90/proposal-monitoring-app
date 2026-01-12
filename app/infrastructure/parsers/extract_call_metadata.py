
import json
from pathlib import Path
from bs4 import BeautifulSoup

def normalize_heading(text: str) -> str:
    if not text:
        return ""
    return text.replace(":", "").strip()

def html_section_to_text(elements):
    """Turn a sequence of <p>, <ul>/<li> etc. into a clean multiline string."""
    parts = []
    for el in elements:
        if el.name == "p":
            txt = el.get_text(" ", strip=True)
            if txt:
                parts.append(txt)
        elif el.name in ("ul", "ol"):
            for li in el.find_all("li", recursive=False):
                bullet = "- " + li.get_text(" ", strip=True)
                parts.append(bullet)
        elif el.name == "li":
            parts.append("- " + el.get_text(" ", strip=True))
    return "\n".join(parts).strip()

def extract_description_fields(html_content: str):
    """
    Parse the HTML from descriptionByte and return sections like 'Scope' and 'Expected Outcome'.
    Handles paragraphs and bullet lists.
    """
    soup = BeautifulSoup(html_content or "", "html.parser")
    results = {}
    # Find all heading spans
    for heading in soup.find_all("span", class_="topicdescriptionkind"):
        key = normalize_heading(heading.get_text(strip=True))
        section_nodes = []
        # collect siblings until the next heading span
        for sib in heading.next_siblings:
            if getattr(sib, "name", None) == "span" and "topicdescriptionkind" in sib.get("class", []):
                break
            if getattr(sib, "name", None) in ("p", "ul", "ol", "li"):
                section_nodes.append(sib)
        results[key] = html_section_to_text(section_nodes)
    return results

def first(seq, default=None):
    if isinstance(seq, list) and seq:
        return seq[0]
    return default

def extract_call_metadata(file_path: Path, call_identifier: str | None = None):
    data = json.loads(Path(file_path).read_text(encoding="utf-8"))
    meta = data.get("raw", {}).get("metadata", {})
    # Identifier
    identifiers = meta.get("identifier") or []
    top_identifier = data.get("identifier")
    identifier = call_identifier or (first(identifiers) if identifiers else top_identifier)
    # Basic fields
    topic_title = first(meta.get("title")) or data.get("summary") or data.get("title")
    call_title = first(meta.get("callTitle"))
    url = data.get("url") or first(meta.get("url"))
    # Dates & status
    start_date = first(meta.get("startDate"))
    deadline_date = first(meta.get("deadlineDate"))
    deadline_model = first(meta.get("deadlineModel"))
    status = first(meta.get("status"))
    types_of_action = first(meta.get("typesOfAction"))
    destination = first(meta.get("destinationDescription"))
    # Budget
    budget_details = None
    budget_str = first(meta.get("budgetOverview"))
    if budget_str:
        try:
            budget = json.loads(budget_str)
            found = False
            for actions in budget.get("budgetTopicActionMap", {}).values():
                for action in actions:
                    action_text = action.get("action", "")
                    if identifier and identifier in action_text:
                        budget_details = action
                        found = True
                        break
                if found:
                    break
        except Exception:
            budget_details = None
    # Description sections
    desc_html = first(meta.get("descriptionByte")) or ""
    sections = extract_description_fields(desc_html)
    result = {
        "identifier": identifier,
        "topic_title": topic_title,
        "call_title": call_title,
        "url": url,
        "destination": destination,
        "type_of_action": types_of_action,
        "status": status,
        "start_date": start_date,
        "deadline_date": deadline_date,
        "deadline_model": deadline_model,
        "expected_outcome": sections.get("Expected Outcome", ""),
        "scope": sections.get("Scope", ""),
        "budget": budget_details,
        "tags": meta.get("tags") or [],
        "keywords": meta.get("keywords") or [],
    }
    return result


def main():
    file_path = Path("app/data/sample.json")
    call_id = "HORIZON-CL2-2025-01-DEMOCRACY-06"
    details = extract_call_metadata(file_path, call_id)

    print(json.dumps(details, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
