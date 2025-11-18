import gspread
from google.oauth2.service_account import Credentials
from loguru import logger

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def connect_to_sheet(sheet_name):
    creds = Credentials.from_service_account_file("creds.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).sheet1
    return sheet

def df_to_google_sheet(df, sheet_name="job_market_data"):
    try:
        sheet = connect_to_sheet(sheet_name)
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        logger.info("[GoogleSheet] Data uploaded successfully!")
        return True
    except Exception as e:
        logger.error(f"[GoogleSheet] Upload failed: {e}")
        return False
