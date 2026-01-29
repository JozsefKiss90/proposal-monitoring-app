import json
import os
from fetch_frameworks import fetch_framework_programme_calls

FRAMEWORK_PROGRAMMES = [
    "43152860",  
]

OUTPUT_DIR = "./output_framework_calls"

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_results = {}

    for fp_id in FRAMEWORK_PROGRAMMES:
        print(f"Fetching frameworkProgramme={fp_id} …")
        calls = fetch_framework_programme_calls(
            framework_programme_id=fp_id,
            page_size=100,
            delay=0.3
        )

        print(f"  → {len(calls)} calls")
        all_results[fp_id] = calls

        out_path = os.path.join(OUTPUT_DIR, f"{fp_id}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(calls, f, indent=2, ensure_ascii=False)

    # Combined file
    combined_path = os.path.join(OUTPUT_DIR, "ALL.json")
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    main()
