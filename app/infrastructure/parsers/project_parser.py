# app/infrastructure/parsers/project_parser.py

def parse_cordis_projects(raw_projects: list[dict]) -> list[dict]:
    parsed = []
    for proj in raw_projects:
        parsed.append({
            "id": proj.get("id"),
            "title": proj.get("title", {}).get("en", "N/A"),
            "objective": proj.get("objective", {}).get("en", "N/A"),
            "start_date": proj.get("startDate"),
            "end_date": proj.get("endDate"),
            "url": proj.get("url", "https://cordis.europa.eu/")
        })
    return parsed