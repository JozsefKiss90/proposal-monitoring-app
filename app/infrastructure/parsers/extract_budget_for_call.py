import json
from pathlib import Path
from typing import Optional, Dict, Any


def extract_budget_for_call(file_path: Path, call_identifier: str) -> Optional[Dict[str, Any]]:
    """
    Extracts the budgetOverview entry for a given call identifier from the provided JSON file.

    Args:
        file_path (Path): Path to the sample.json file
        call_identifier (str): The identifier of the call (e.g., "HORIZON-CL2-2025-01-DEMOCRACY-06")

    Returns:
        dict | None: A dictionary containing the matched budget action or None if not found
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Step 1: Access embedded string in metadata.budgetOverview[0]
        embedded_json_str = data["raw"]["metadata"]["budgetOverview"][0]
        budget_json = json.loads(embedded_json_str)

        # Step 2: Search for the identifier in action fields
        for topic_id, actions in budget_json.get("budgetTopicActionMap", {}).items():
            for action in actions:
                if call_identifier in action.get("action", ""):
                    return action  # Return the first matched budget entry

    except Exception as e:
        print(f"Error: {e}")
        return None

    return None  # No match found


def main():
    sample_file_path = Path("app/data/sample.json")
    call_id = "HORIZON-CL2-2025-01-DEMOCRACY-06"
    result = extract_budget_for_call(sample_file_path, call_id)

    if result:
        print(f"✅ Budget data for {call_id}:\n")
        for key, value in result.items():
            print(f"{key}: {value}")
    else:
        print(f"❌ No budget data found for {call_id}")


if __name__ == "__main__":
    main()
