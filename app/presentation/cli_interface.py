# app/presentation/cli_interface.py

import json
from app.application.orchestrator import run_cordis_pipeline

if __name__ == "__main__":
    results = run_cordis_pipeline()

    with open("cordis_summaries.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(results)} summaries to cordis_summaries.json")
