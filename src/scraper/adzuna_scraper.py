import os
import math
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
import re

load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
COUNTRY = os.getenv("ADZUNA_COUNTRY", "in")   # in for India
RESULTS_PER_PAGE = 50   # Adzuna max: 50

if not APP_ID or not APP_KEY:
    raise ValueError("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in .env")

logger.add("logs/adzuna.log", rotation="1 day")

def extract_experience(text):
    if not isinstance(text, str):
        return None

    text = text.lower()

    patterns = [
        r'(\d+)\+?\s*years? of experience',
        r'(\d+)\s*-\s*(\d+)\s*years?.*experience',
        r'minimum\s*(\d+)\s*years',
        r'at least\s*(\d+)\s*years',
        r'(\d+)\+?\s*years?',
        r'(\d+)\s*yrs'
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            if len(match.groups()) == 2:
                low, high = match.groups()
                return f"{low}-{high}"
            return match.group(1)

    implied = {
        "entry-level": "0-1",
        "fresher": "0",
        "junior": "0-1",
        "mid-level": "2-4",
        "senior": "5+"
    }
    for phrase, exp in implied.items():
        if phrase in text:
            return exp

    return None

def fetch_adzuna(keyword, page=1, results_per_page=RESULTS_PER_PAGE, country=COUNTRY):
    """
    Fetch one page of results from Adzuna.
    page is 1-indexed for Adzuna.
    """
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": results_per_page,
        "what": keyword,
        "content-type": "application/json"
    }
    logger.info(f"[Adzuna] Requesting page {page} for '{keyword}'")
    resp = requests.get(url, params=params, timeout=15)
    if resp.status_code != 200:
        logger.warning(f"[Adzuna] status={resp.status_code} body={resp.text[:200]}")
        return None
    return resp.json()

def scrape_adzuna(max_pages=2, country=COUNTRY):
    TECH_KEYWORDS = [
        "software developer", "software engineer", "full stack developer",
        "frontend developer", "backend developer", "android developer",
        "ios developer", "data analyst", "data scientist",
        "ml engineer", "machine learning", "ai engineer",
        "devops engineer", "cloud engineer", "site reliability engineer",
        "database administrator", "data engineer", "business analyst",
        "cyber security", "qa engineer", "tester", "automation engineer"
    ]

    all_jobs = []

    logger.info(f"[Adzuna] Starting full tech scrape across {len(TECH_KEYWORDS)} keywords")

    for keyword in TECH_KEYWORDS:
        logger.info(f"[Adzuna] Scraping keyword: {keyword}")

        # First request
        res = fetch_adzuna(keyword, page=1)
        if not res:
            continue

        total_results = res.get("count", 0)
        pages = min(max_pages, math.ceil(total_results / RESULTS_PER_PAGE or 1))
        results = res.get("results", [])

        # Process page 1
        for item in results:
            desc = item.get("description") or ""
            job = {
                "keyword": keyword,
                "title": item.get("title"),
                "company": item.get("company", {}).get("display_name"),
                "location": item.get("location", {}).get("display_name"),
                "description": desc,
                "experience_required": extract_experience(desc),
                "created": item.get("created"),
                "redirect_url": item.get("redirect_url"),
            }
            all_jobs.append(job)

    df = pd.DataFrame(all_jobs)
    os.makedirs("data_raw", exist_ok=True)
    df.to_csv("data_raw/adzuna_jobs.csv", index=False)
    logger.info(f"[Adzuna] Saved {len(df)} rows to data_raw/adzuna_jobs.csv")
    return df


