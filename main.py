import gspread
import pandas as pd
from google.oauth2 import service_account

credentials_file_path = './key/credentials.json'

credentials = service_account.Credentials.from_service_account_file(
    credentials_file_path
)

id_sheet = "1q0dPc8gs9Wxnp6RIM1lsOJKrhlmMgljrI4z9SRc2O1c"

gc = gspread.service_account(filename=credentials_file_path)

wks = gc.open_by_key(id_sheet)
wks_list = [sheet.title for sheet in wks.worksheets()]
# ["2021","2022","2023"]

df_list = list() # lista iniciada vazia neste primeiro momento

for sheet in wks_list:
    data = wks.worksheet(sheet).get_all_values()
    df = pd.DataFrame(data=data[1:], columns=data[0])
    df_list.append(df)

# [df21,df22,df23] -> unificar esta lista em um único dataframe
df_sales = pd.concat(df_list, axis=0, ignore_index=True)

project_id = "pelagic-cycle-402719"
dataset_id = "google_sheets"
table_name = "sales"

# projeto.dataset.tabela
df_sales.to_gbq(
    credentials=credentials,
    destination_table=f"{project_id}.{dataset_id}.{table_name}",
    project_id=project_id,
    if_exists="replace",
    progress_bar=True
)