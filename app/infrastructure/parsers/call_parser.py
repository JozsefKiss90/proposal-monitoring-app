# app/infrastructure/parsers/call_parser.py

def parse_calls(soup) -> list[dict]:
    calls = []
    # Extract call titles, deadlines, descriptions, etc.
    # Placeholder example:
    for item in soup.select(".call-item"):
        calls.append({
            "title": item.select_one(".title").text.strip(),
            "deadline": item.select_one(".deadline").text.strip(),
            "url": item.select_one("a")["href"],
        })
    return calls
