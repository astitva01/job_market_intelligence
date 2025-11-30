import gspread
from google.oauth2.service_account import Credentials
from loguru import logger

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def connect_to_sheet(spreadsheet_id, sheet_name):
    creds = Credentials.from_service_account_file("creds.json", scopes=SCOPES)
    client = gspread.authorize(creds)

    # open the spreadsheet
    spreadsheet = client.open_by_key(spreadsheet_id)

    # try to open worksheet, else create
    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)

    return sheet


def df_to_google_sheet(df, sheet_name, spreadsheet_id):
    try:
        sheet = connect_to_sheet(spreadsheet_id, sheet_name)
        sheet.clear()

        # Push column names + data
        sheet.update([df.columns.values.tolist()] + df.values.tolist())

        logger.info("[GoogleSheet] Data uploaded successfully!")
        return True

    except Exception as e:
        logger.error(f"[GoogleSheet] Upload failed: {e}")
        return False
