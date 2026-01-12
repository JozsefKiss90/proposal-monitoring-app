FROM python:3.11-slim

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y \
    wget curl unzip git \
    libglib2.0-0 libnss3 libgdk-pixbuf-2.0-0 libgtk-3-0 libx11-xcb1 \
    libxcomposite1 libxcursor1 libxdamage1 libxi6 libxtst6 libdrm2 \
    libgbm1 libasound2 libxrandr2 libatk-bridge2.0-0 libxss1 libxext6 \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install --with-deps

# Set working directory and copy source
WORKDIR /app
COPY . .

# Set command to run scraper
CMD ["python", "app/infrastructure/scrapers/cordis_scraper.py"]
