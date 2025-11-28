from schedules.download.DownloadSchedule import DownloadSchedule
from schedules.parse.ScheduleParser import ScheduleParser
from schedules.stage_transform.StageTransformer import StageTransformer
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from openpyxl import load_workbook
import pandas as pd
import shutil


MANUF_NAME = "Dembla"
CRON_EXP = "0 6-15/2 * * mon-fri"
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "Artem",
    "retries": 0
}


@task
def download_and_insert():
    _created_at = datetime.now()

    ds = DownloadSchedule("tmp")
    ds.download_by_manuf_name(manuf_name=MANUF_NAME)

    workbook = load_workbook(f"tmp/{MANUF_NAME}.xlsx", data_only=True, read_only=True)
    sp = ScheduleParser(workbook=workbook)
    
    stage_transformer = StageTransformer()

    values_to_insert = []
    colors_to_insert = []
    for sheet_name in sp.project_sheets:
        print(sheet_name)
        c = sp.project_sheets[sheet_name]["columns"] # pg format
        v = sp.project_sheets[sheet_name]["values"]
        b = sp.project_sheets[sheet_name]["background"]

        # Работаем со значениями
        values_df = pd.DataFrame(v, columns=c)
        values_df = stage_transformer.kks(values_df)
        values_df = stage_transformer.row_number(values_df)
        values_to_insert.append((
            _created_at,
            values_df.to_json(orient='records', date_format='iso')
        ))

        # Работаем с цветами
        colors_df = pd.DataFrame(b, columns=c)
        colors_df = stage_transformer.row_number(colors_df)
        colors_to_insert.append((
            _created_at,
            colors_df.to_json(orient='records', date_format='iso')
        ))
    
    # Загружаем данные в PG
    hook = PostgresHook(postgres_conn_id="resdb_connection")
    
    conn = hook.get_conn()
    cursor = conn.cursor()
        
    # SQL запрос для вставки
    values_query = f"""
    INSERT INTO test (_created_at, content)
    VALUES (%s, %s)
    """
    colors_query = f"""
    INSERT INTO test (_created_at, content)
    VALUES (%s, %s)
    """
    
    # Массовая вставка
    cursor.executemany(values_query, values_to_insert)
    cursor.executemany(colors_query, colors_to_insert)
    conn.commit()
    cursor.close()
    conn.close()

    # hook.insert_rows(
    #     table="stage_dembla_values",
    #     rows=[tuple(x) for x in values_df.to_records(index=False)],
    #     columns=list(values_df.columns)
    # )

    # hook.insert_rows(
    #     table="stage_dembla_colors",
    #     rows=[tuple(x) for x in colors_df.to_records(index=False)],
    #     columns=list(colors_df.columns)
    # )

    shutil.rmtree("tmp")


with DAG(
    dag_id="stage_dembla",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    download_and_insert()

