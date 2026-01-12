# app/application/orchestrator.py

from app.infrastructure.scrapers.cordis_scraper import fetch_cordis_projects
from app.infrastructure.parsers.project_parser import parse_cordis_projects
from app.domain.summarizer import summarize_projects

def run_cordis_pipeline():
    raw = fetch_cordis_projects(limit=3)
    parsed = parse_cordis_projects(raw)
    summarized = summarize_projects(parsed)
    return summarized
