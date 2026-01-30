from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
from pyairtable import Table, Api
from gspread import service_account


SERVICE_ACCOUNT_CREDS_PATH = "./plugins/schedules/download/submitted-tables-download-v02-750e825a7950.json"
SHHETS_URL = "https://docs.google.com/spreadsheets/d/1Clbm3ie2e8HqLjHu0FLJVMUt51Y_4Fgb8Q8O6WfbpJw"

def insert_to_gs(data_to_insert, sheet_name):
    client = service_account(SERVICE_ACCOUNT_CREDS_PATH)

    worksheet = client.open_by_url(SHHETS_URL).worksheet(sheet_name)
    # client.open_by_url(LEADS_URL).add_worksheet(title="New sheet", rows=100, cols=20)

    row_start = 2
    col_start = 1

    worksheet.update(
        f"R{row_start}C{col_start}:R{row_start + len(data_to_insert) - 1}C{col_start + len(data_to_insert[0]) - 1}",
        data_to_insert,
        raw=False
    )

@task
def main_task():

    hook = PostgresHook(postgres_conn_id="resdb_connection")

    records = hook.get_records("""
        SELECT 
            _created_at,
            _manuf,
            _sheet_name,
            replace(po_item, ',', '.'),
            key,
            try_parse_date(old_value),
            try_parse_date(current_value)
        FROM schedules_changes
        WHERE try_parse_date(old_value) IS NOT NULL
          AND try_parse_date(current_value) IS NOT NULL
    """)

    records = [{
        "_created_at": row[0],
        "_manuf": row[1],
        "_sheet_name": row[2],
        "po_item": row[3],
        "key": row[4],
        "old_value": row[5],
        "current_value": row[6]
        } 
    for row in records]

    insert_to_gs(records, "Changes")

with DAG(
    dag_id="icp_metrics_sheet",
    default_args={"owner": "Artem", "retries": 0},
    start_date=datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval=None,
    catchup=False
):
    main_task()