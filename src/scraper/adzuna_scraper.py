# src/scraper/adzuna_scraper.py
import os
import math
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
COUNTRY = os.getenv("ADZUNA_COUNTRY", "in")   # in for India, uk, us (use appropriate code)
RESULTS_PER_PAGE = 50   # Adzuna max: 50

if not APP_ID or not APP_KEY:
    raise ValueError("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in .env")

logger.add("logs/adzuna.log", rotation="1 day")

def extract_skills_from_text(text, skill_master=None):
    if not text:
        return None
    if skill_master is None:
        skill_master = ["python","sql","excel","tableau","power bi","aws","java","spark","snowflake","mongodb","etl","pandas","numpy","machine learning","tensorflow"]
    lower = text.lower()
    found = [s for s in skill_master if s in lower]
    return ", ".join(found) if found else None

def extract_salary_min_max(item):
    # Adzuna returns salary_min and salary_max in item if available
    smin = item.get("salary_min")
    smax = item.get("salary_max")
    try:
        return float(smin) if smin else None, float(smax) if smax else None
    except:
        return None, None

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

def scrape_adzuna(keyword="data analyst", max_pages=2, country=COUNTRY):
    """
    Fetch multiple pages and return DataFrame.
    Use max_pages small (1-5) on testing.
    """
    all_jobs = []
    # first call to get count & pages
    res = fetch_adzuna(keyword, page=1)
    if not res:
        return pd.DataFrame(all_jobs)

    total_results = res.get("count", 0)
    logger.info(f"[Adzuna] total_results={total_results}")
    pages = min(max_pages, math.ceil(total_results / RESULTS_PER_PAGE or 1))

    # process first page results
    results = res.get("results", [])
    for item in results:
        smin, smax = extract_salary_min_max(item)
        description = item.get("description") or ""
        job = {
            "title": item.get("title"),
            "company": item.get("company", {}).get("display_name"),
            "location": item.get("location", {}).get("display_name"),
            "description": description,
            "salary_min": smin,
            "salary_max": smax,
            "created": item.get("created"),
            "redirect_url": item.get("redirect_url"),
            "skills": extract_skills_from_text(description + " " + str(item.get("title")))
        }
        all_jobs.append(job)

    # other pages
    for p in range(2, pages + 1):
        time.sleep(1.0)  # polite
        res_p = fetch_adzuna(keyword, page=p)
        if not res_p:
            continue
        for item in res_p.get("results", []):
            smin, smax = extract_salary_min_max(item)
            description = item.get("description") or ""
            job = {
                "title": item.get("title"),
                "company": item.get("company", {}).get("display_name"),
                "location": item.get("location", {}).get("display_name"),
                "description": description,
                "salary_min": smin,
                "salary_max": smax,
                "created": item.get("created"),
                "redirect_url": item.get("redirect_url"),
                "skills": extract_skills_from_text(description + " " + str(item.get("title")))
            }
            all_jobs.append(job)

    df = pd.DataFrame(all_jobs)
    os.makedirs("data_raw", exist_ok=True)
    df.to_csv("data_raw/adzuna_jobs.csv", index=False)
    logger.info(f"[Adzuna] Saved {len(df)} rows to data_raw/adzuna_jobs.csv")
    return df
