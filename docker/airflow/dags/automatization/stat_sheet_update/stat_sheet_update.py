from gspread import service_account
import pandas as pd
import yaml
from airflow.decorators import task
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook


SERVICE_ACCOUNT_CREDS_PATH = "./src/schedules/download/submitted-tables-download-v02-750e825a7950.json"
CONFIG_PATH = './src/schedules/CONFIG.yaml'
SHEET_UPDATE_QUERY = """
SELECT
    _manuf,
    _sheet_name,
    TO_CHAR(T_created_at, "DD.MM.YYYY") AS last_update_time
FROM (
    SELECT
        _created_at,
        _manuf,
        _sheet_name,
        content,
        LAG(content) OVER(PARTITION BY _manuf, _sheet_name ORDER BY _created_at) AS prev_content
    FROM stage.schedule_changes
) AS t1
WHERE content != prev_content
GROUP BY _manuf, _sheet_name
ORDER BY _manuf, _sheet_name
"""

@task
def main_task():

    # === Подключаемся к БД ===
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    # === Получаем данные и закрываем подключение ===
    conn = hook.get_conn()
    records = hook.get_records(SHEET_UPDATE_QUERY)
    conn.close()

    # === Парсим данные ===
    df = pd.DataFrame([{
        "manuf": r[0],
        "sheet_name": r[1],
        "last_update_time": r[2]
    } for r in records])

    # === Подключаемся к гугл аккаунту ===
    client = service_account(SERVICE_ACCOUNT_CREDS_PATH)

    # === Читаем конфиг с ссылками на schedules ===
    with open(CONFIG_PATH) as file:
        urls = yaml.safe_load(file)["URLS"]
    
    for manuf in df["manuf"].unique():
        if manuf == "RKC":
            manuf_records = df[df["manuf"] == manuf]
            data_to_insert = manuf_records[["sheet_name", "last_update_time"]].to_list()

            stat_sheet = client.open_by_url(urls[manuf]).worksheet("Stat")

            row_start = 2
            col_start = 1

            stat_sheet.batch_clear([
                f"R{row_start}C{col_start}:R{row_start + len(data_to_insert) - 1}C{col_start + len(data_to_insert[0]) - 1}"
            ])

            stat_sheet.update(
                f"R{row_start}C{col_start}:R{row_start + len(data_to_insert) - 1}C{col_start + len(data_to_insert[0]) - 1}",
                data_to_insert,
                raw=False
            )

CRON_EXP = "0 7-16/2 * * mon-fri"
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "Artem",
    "retries": 0
}

with DAG(
    dag_id="stat_sheet_update",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    
    main_task()
    
# if __name__ == "__main__":

#     # === Подключаемся к гугл аккаунту ===
#     client = service_account("/Users/artem/Desktop/Artem/Work/RES/DWH/src/schedules/download/submitted-tables-download-v02-750e825a7950.json")

#     # === Читаем конфиг с ссылками на schedules ===
#     with open("/Users/artem/Desktop/Artem/Work/RES/DWH/src/schedules/CONFIG.yaml") as file:
#         urls = yaml.safe_load(file)["URLS"]

#     stat_sheet = client.open_by_url(urls["RKC"]).worksheet("Stat")

#     data_to_insert = [["1", "123"], ["2", "t4wt2e"]]
    
#     row_start = 2
#     col_start = 1

#     stat_sheet.batch_clear([
#         f"R{row_start}C{col_start}:R{row_start + len(data_to_insert) - 1}C{col_start + len(data_to_insert[0]) - 1}"
#     ])

#     stat_sheet.update(
#         f"R{row_start}C{col_start}:R{row_start + len(data_to_insert) - 1}C{col_start + len(data_to_insert[0]) - 1}",
#         data_to_insert,
#         raw=False
#     )
