from playwright.sync_api import sync_playwright
import re
import os
import json

def get_project_ids(limit=10):
    url = "https://cordis.europa.eu/search?q=contenttype%3D%27project%27%20AND%20frameworkProgramme%3D%27HORIZON%27%20AND%20(%2Fproject%2Frelations%2Fcategories%2FeuroSciVoc%2Fcode%3D%27%2F23%2F%27%20OR%20%2Fproject%2Frelations%2Fcategories%2FeuroSciVoc%2Fcode%3D%3D%27%2F23%27%20OR%20%2Fproject%2Frelations%2Fcategories%2FeuroSciVoc%2Fcode%3D%27%2F23%2F47%2F%27%20OR%20%2Fproject%2Frelations%2Fcategories%2FeuroSciVoc%2Fcode%3D%3D%27%2F23%2F47%27)&p=1&num=50&srt=Relevance:decreasingng"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print(f"[INFO] Loading page: {url}")
        page.goto(url, timeout=60000)
        page.wait_for_timeout(10000)
        visible_text = page.inner_text("body")
        browser.close()

    # Extract project IDs
    project_ids = re.findall(r'ID:\s*(\d+)', visible_text)
    project_ids = list(dict.fromkeys(project_ids))  # Deduplicate

    print(f"[INFO] Found {len(project_ids)} project IDs.")
    for i, pid in enumerate(project_ids[:limit]):
        print(f"[{i+1}] {pid}")

    # Save to JSON
    output_dir = os.path.join("app", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "project_ids.json")

    with open(output_path, "w") as f:
        json.dump(project_ids[:limit], f, indent=2)

    print(f"[INFO] Saved project IDs to {output_path}")
    return project_ids[:limit]

if __name__ == "__main__":
    get_project_ids(limit=20)
