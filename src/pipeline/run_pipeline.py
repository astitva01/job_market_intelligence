# import pandas as pd
# from src.scraper.indeed_serpapi import scrape_indeed_serpapi
# from src.database.google_sheet_writer import df_to_google_sheet

# def run_pipeline():
#     print("🚀 Starting Job Market Intelligence Pipeline")

#     df = scrape_indeed_serpapi("data analyst", pages=3)

#     print("📤 Uploading to Google Sheet...")
#     df_to_google_sheet(
#         spreadsheet_id="1PhN_0-n-u4Gbt7UbBp2zTh_sa4d1f6_flSbRdHLYnLc",
#         sheet_name="job_market_data",
#         df=df
#     )

#     print("✅ Pipeline completed successfully!")

# if __name__ == "__main__":
#     run_pipeline()

# src/pipeline/run_pipeline.py
import os
from dotenv import load_dotenv
from src.scraper.adzuna_scraper import scrape_adzuna
from src.database.google_sheet_writer import df_to_google_sheet

load_dotenv()

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Adzuna Jobs")

def run_pipeline():
    print("🚀 Starting Adzuna Job Pipeline")
    df = scrape_adzuna("data analyst", max_pages=2)   # start with 2 pages
    print(f"📦 Scraped {len(df)} jobs")
    if len(df) == 0:
        print("⚠️ No jobs scraped. Check Adzuna credentials or change the keyword.")
    else:
        ok = df_to_google_sheet(df=df, sheet_name=SHEET_NAME, spreadsheet_id=SPREADSHEET_ID)
        print("Upload OK?", ok)
    print("✅ Pipeline finished")

if __name__ == "__main__":
    run_pipeline()
