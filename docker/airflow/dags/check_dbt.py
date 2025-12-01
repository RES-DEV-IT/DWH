from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow import DAG


CRON_EXP = None
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "Artem",
    "retries": 0
}

with DAG(
    dag_id="check_dbt",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_operator="cd dbt_project && dbt run -m my_first_dbt_model",
        env={
            "DBT_PROFILES_DIR": "dbt_project"
        }
    )