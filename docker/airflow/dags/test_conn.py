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


CRON_EXP = None
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "DEV",
    "retries": 0
}

@task
def check_conn_task():
    # Загружаем данные в PG
    hook = PostgresHook(postgres_conn_id="resdb_connection")

with DAG(
    dag_id="check_conn",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    check_conn_task()
    