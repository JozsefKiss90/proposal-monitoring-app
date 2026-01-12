from playwright.sync_api import sync_playwright
import json
import os

def scrape_project_text(project_id):
    url = f"https://cordis.europa.eu/project/id/{project_id}"
    print(f"[INFO] Loading project page: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60000)
        page.wait_for_timeout(10000)

        # Get visible text only (as seen on the screen)
        visible_text = page.inner_text("body")

        browser.close()

    # Clean up output: remove long CSS blocks and repeated newlines
    lines = visible_text.splitlines()
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        if (
            line and
            not line.lower().startswith("@charset") and
            not line.lower().startswith(":root") and
            not line.startswith("/*") and
            not line.endswith("{")
        ):
            cleaned_lines.append(line)

    print("\n[INFO] === VISIBLE PROJECT TEXT ===\n")
    for line in cleaned_lines[:50]:  # limit output for readability
        print(f"- {line}")
    print("\n[INFO] === END ===")

    return cleaned_lines

def parse_project_data(lines):
    data = {
        "id": None,
        "title": None,
        "start_date": None,
        "end_date": None,
        "coordinator": None,
        "country": None,
        "eu_contribution": None,
        "total_cost": None,
        "objective": None,
        "programme": None,
        "call": None,
        "topic": None,
        "participants": [],
    }

    i = 0
    while i < len(lines):
        line = lines[i]

        # Title (longest line before "Fact Sheet")
        if line == "Fact Sheet" and i > 0:
            data["title"] = lines[i - 1]

        if line.startswith("Grant agreement ID:"):
            data["id"] = line.split(":")[-1].strip()

        elif line == "Start date" and i + 1 < len(lines):
            data["start_date"] = lines[i + 1]

        elif line == "End date" and i + 1 < len(lines):
            data["end_date"] = lines[i + 1]

        elif line == "Coordinated by" and i + 2 < len(lines):
            data["coordinator"] = lines[i + 1]
            data["country"] = lines[i + 2]

        elif line == "Total cost" and i + 1 < len(lines):
            data["total_cost"] = lines[i + 1]

        elif line == "EU contribution" and i + 1 < len(lines):
            data["eu_contribution"] = lines[i + 1]

        elif line == "Objective":
            # Collect multiline objective until next known section
            objective_lines = []
            j = i + 1
            while j < len(lines) and not lines[j].strip().endswith(":") and not lines[j].strip() in [
                "Fields of science (EuroSciVoc)", "Keywords", "Programme(s)", "Topic(s)", "Call for proposal"
            ]:
                objective_lines.append(lines[j])
                j += 1
            data["objective"] = " ".join(objective_lines)
            i = j - 1  # continue from the last line of objective

        elif line == "Programme(s)" and i + 1 < len(lines):
            data["programme"] = lines[i + 1]

        elif line == "Call for proposal" and i + 1 < len(lines):
            data["call"] = lines[i + 1]

        elif line == "Topic(s)" and i + 1 < len(lines):
            data["topic"] = lines[i + 1]

        elif line == "Beneficiaries (12)":
            # Read all beneficiaries until footer
            j = i + 1
            while j + 2 < len(lines) and "Share this page" not in lines[j]:
                name = lines[j].strip()
                country = lines[j + 1].strip()
                contribution = lines[j + 2].strip()
                if name and country and contribution.startswith("€"):
                    data["participants"].append({
                        "name": name,
                        "country": country,
                        "contribution": contribution
                    })
                    j += 3
                else:
                    j += 1
            break  # we've got all we need

        i += 1

    return data

if __name__ == "__main__":
    # Load IDs from app/data/project_ids.json
    with open("app/data/project_ids.json", "r") as f:
        project_ids = json.load(f)

    results = []
    for pid in project_ids[:5]:  # process only first 5
        try:
            lines = scrape_project_text(pid)
            structured = parse_project_data(lines)
            results.append(structured)
            print(f"[INFO] ✅ Scraped project ID: {pid}")
        except Exception as e:
            print(f"[ERROR] Failed on {pid}: {e}")

    # Save to app/data/projects.json
    os.makedirs("app/data", exist_ok=True)
    output_path = "app/data/projects.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"[INFO] Saved {len(results)} projects to {output_path}")


