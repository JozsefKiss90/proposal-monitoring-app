# app/infrastructure/services/openrouter_client.py

import requests
import os

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

def summarize_text(text: str, prompt: str = "Summarize the following project:") -> str:
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "mistralai/mistral-7b-instruct:free",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": text}
        ],
        "temperature": 0.5,
        "max_tokens": 512
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    return response.json()["choices"][0]["message"]["content"]
