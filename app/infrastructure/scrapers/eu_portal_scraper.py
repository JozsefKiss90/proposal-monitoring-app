# app/infrastructure/scrapers/eu_portal_scraper.py

import requests
from bs4 import BeautifulSoup

def fetch_calls():
    url = "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-search"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to fetch EU Portal data.")
    
    soup = BeautifulSoup(response.text, "html.parser")
    # placeholder: extract relevant HTML chunks
    return soup
