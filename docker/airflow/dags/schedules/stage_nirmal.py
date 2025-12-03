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


MANUF_NAME = "Nirmal"
CRON_EXP = "15 6-15/2 * * mon-fri"
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "Artem",
    "retries": 0
}


@task
def download_and_insert():
    _created_at = datetime.now()

    ds = DownloadSchedule()
    result = ds.download_by_manuf_name(manuf_name=MANUF_NAME)
    stage_transformer = StageTransformer()

    values_to_insert = []
    colors_to_insert = []
    for sheet_name in result:
        print(sheet_name)
        c = result[sheet_name]["columns"] # pg format
        v = result[sheet_name]["values"]
        b = result[sheet_name]["background"]

        # Работаем со значениями
        values_df = pd.DataFrame(v, columns=c)
        values_df = stage_transformer.kks(values_df)
        values_df = stage_transformer.row_number(values_df)
        values_to_insert.append((
            _created_at,
            MANUF_NAME,
            sheet_name,
            values_df.to_json(orient='records', date_format='iso')
        ))

        # Работаем с цветами
        colors_df = pd.DataFrame(b, columns=c)
        colors_df = stage_transformer.row_number(colors_df)
        colors_to_insert.append((
            _created_at,
            MANUF_NAME,
            sheet_name, 
            colors_df.to_json(orient='records', date_format='iso')
        ))
    
    # Загружаем данные в PG
    hook = PostgresHook(postgres_conn_id="resdb_connection")
    
    conn = hook.get_conn()
    cursor = conn.cursor()
        
    # SQL запрос для вставки
    values_query = f"""
    INSERT INTO stage.schedules_values (_created_at, _manuf, _sheet_name, content)
    VALUES (%s, %s, %s, %s)
    """
    colors_query = f"""
    INSERT INTO stage.schedules_colors (_created_at, _manuf, _sheet_name, content)
    VALUES (%s, %s, %s, %s)
    """
    
    # Массовая вставка
    cursor.executemany(values_query, values_to_insert)
    cursor.executemany(colors_query, colors_to_insert)
    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id=f"stage_{MANUF_NAME.lower()}",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    download_and_insert()
