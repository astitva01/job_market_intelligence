import hashlib
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

from .utils import safe_request, random_delay, build_indeed_url, clean_text
from .proxies import get_working_proxy
from ..database.google_sheet_writer import df_to_google_sheet

def make_job_hash(title, company, location, description):
    base = "|".join([
        (title or "").lower(),
        (company or "").lower(),
        (location or "").lower(),
        (description or "").lower()
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:48]

def extract_salary_numeric(s):
    if not s:
        return None
    import re
    nums = re.findall(r"[\d,]+", s.replace("₹",""))
    if not nums:
        return None
    try:
        return float(nums[0].replace(",", ""))
    except:
        return None

def extract_skills(text):
    if not text:
        return None
    skills = ["python","sql","excel","tableau","power bi","aws","java","spark","snowflake","mongodb","rest api"]
    found = [s for s in skills if s.lower() in text.lower()]
    return ", ".join(found) if found else None

def scrape_indeed(keyword, pages=5, use_proxy=False):
    logger.info(f"[scraper] Starting Indeed scrape for: {keyword}")
    proxy = get_working_proxy() if use_proxy else None

    all_jobs = []

    for page in range(0, pages):
        url = build_indeed_url(keyword, start=page*10)
        html = safe_request(url, proxies=proxy)

        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("td", {"class": "resultContent"})

        for c in cards:
            title_tag = c.find("h2", {"class": "jobTitle"})
            company_tag = c.find("span", {"class": "companyName"})
            location_tag = c.find("div", {"class": "companyLocation"})
            salary_tag = c.find("div", {"class": "salary-snippet"})
            desc_tag = c.find("div", {"class": "job-snippet"})

            title = clean_text(title_tag.text) if title_tag else None
            company = clean_text(company_tag.text) if company_tag else None
            location = clean_text(location_tag.text) if location_tag else None
            salary = clean_text(salary_tag.text) if salary_tag else None
            description = clean_text(desc_tag.text) if desc_tag else None

            job = {
                "title": title,
                "company": company,
                "location": location,
                "salary": salary,
                "salary_numeric": extract_salary_numeric(salary),
                "description": description,
                "skills": extract_skills((title or "") + " " + (description or "")),
                "job_hash": make_job_hash(title, company, location, description)
            }

            all_jobs.append(job)

        random_delay(1.5, 3.5)

    df = pd.DataFrame(all_jobs)
    df.to_csv("data_raw/scraped_jobs.csv", index=False)
    logger.info("[scraper] Scraping finished. Saved CSV.")
    return df

if __name__ == "__main__":
    df = scrape_indeed("data analyst", pages=5)
    df_to_google_sheet(df, sheet_name="job_market_data")
