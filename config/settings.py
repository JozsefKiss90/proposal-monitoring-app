# config/settings.py

import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    EU_PORTAL_BASE_URL = os.getenv("EU_PORTAL_BASE_URL", "https://ec.europa.eu/info/funding-tenders")
    CORDIS_API_URL = os.getenv("CORDIS_API_URL", "https://cordis.europa.eu/api")
    HORIZON_API_URL = os.getenv("HORIZON_API_URL", "https://webgate.ec.europa.eu/dashboard/api")
    
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/proposals")

settings = Settings()
