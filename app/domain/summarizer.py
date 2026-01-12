# app/domain/summarizer.py

from app.infrastructure.services.openrouter_client import summarize_text

def summarize_projects(projects: list[dict]) -> list[dict]:
    for proj in projects:
        text = f"Title: {proj['title']}\nObjective: {proj['objective']}"
        summary = summarize_text(text)
        proj["summary"] = summary
    return projects
