import json
import requests
from pathlib import Path
import time

def fetch_call_data(call_ids, output_path, delay=0.5):
    base_url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
    results = []

    for i, call_id in enumerate(call_ids, 1):
        params = {
            "apiKey": "SEDIA",
            "text": f'"{call_id}"'
        }

        try:
            response = requests.post(base_url, params=params)  # ✅ correct usage
            response.raise_for_status()
            data = response.json()

            for result in data.get("results", []):
                results.append({
                    "identifier": call_id,
                    "title": result.get("title"),
                    "summary": result.get("summary"),
                    "url": result.get("url"),
                    "raw": result
                })

            print(f"[{i}/{len(call_ids)}] ✅ Fetched: {call_id}")
        except Exception as e:
            print(f"[{i}/{len(call_ids)}] ❌ Failed for {call_id}: {e}")

        time.sleep(delay)

    # Save output
    with open(output_path, "w", encoding="utf-8") as out:
        json.dump(results, out, indent=2)
    print(f"\n✅ Saved metadata to: {output_path}")

def main():
    input_path = "app/data/horizon_cl_all_2025_calls.json"
    output_path = "app/data/fetched_call_metadata.json"

    with open(input_path, "r", encoding="utf-8") as f:
        call_ids = json.load(f)

    fetch_call_data(call_ids[:30], output_path)

if __name__ == "__main__":
    main()
