from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
from pyairtable import Table, Api
from gspread import service_account
from plugins.icp_metrics import shifts, kks_vs_qty


SERVICE_ACCOUNT_CREDS_PATH = "./plugins/schedules/download/submitted-tables-download-v02-750e825a7950.json"
SHHETS_URL = "https://docs.google.com/spreadsheets/d/1Clbm3ie2e8HqLjHu0FLJVMUt51Y_4Fgb8Q8O6WfbpJw"

def insert_to_gs(data_to_insert, sheet_name):
    client = service_account(SERVICE_ACCOUNT_CREDS_PATH)

    worksheet = client.open_by_url(SHHETS_URL).worksheet(sheet_name)
    # client.open_by_url(LEADS_URL).add_worksheet(title="New sheet", rows=100, cols=20)

    row_start = 2
    col_start = 1

    # worksheet.update(
    #     f"R{row_start}C{col_start}:R{row_start + len(data_to_insert) - 1}C{col_start + len(data_to_insert[0]) - 1}",
    #     data_to_insert,
    #     raw=False
    # )

    worksheet.append_rows(
        data_to_insert,
        value_input_option="USER_ENTERED"
    )

@task
def main_task():

    hook = PostgresHook(postgres_conn_id="resdb_connection")

    for manuf_name in ["DelVal", "Dembla", "HawaTubes", "LC", "RKC", "Nirmal", "EHO"]:
        records = shifts(hook, manuf_name)

        insert_to_gs(records, f"Changes {manuf_name}")

    df = kks_vs_qty(hook)
    insert_to_gs(df.values.tolist(), "KKS vs QTY")

with DAG(
    dag_id="icp_metrics_sheet",
    default_args={"owner": "Artem", "retries": 0},
    start_date=datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval="30 6-15/2 * * mon-fri",
    catchup=False
):
    main_task()
