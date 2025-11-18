import pandas as pd
from src.database.google_sheet_writer import df_to_google_sheet
df = pd.DataFrame([{"title":"test","company":"me","location":"patna"}])
print(df_to_google_sheet(df, sheet_name="job_market_data"))
