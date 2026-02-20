from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow import DAG


CRON_EXP = None
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)
DBT_MODEL_NAME_VALUES = "int_schedules_values"
DBT_MODEL_NAME_COLORS = "int_schedules_colors"

default_args = {
    "owner": "DEV",
    "retries": 0
}


with DAG(
    dag_id="intermidiate_schedules",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    
    dbt_run_values = BashOperator(
        task_id="dbt_run_values",
        bash_command=f"/home/airflow/.local/bin/dbt run -m {DBT_MODEL_NAME_VALUES}",
        cwd='/opt/airflow/plugins/dbt_project',
        env={
            "DBT_PROFILES_DIR": "."
        }
    )

    dbt_run_colors = BashOperator(
        task_id="dbt_run_colors",
        bash_command=f"/home/airflow/.local/bin/dbt run -m {DBT_MODEL_NAME_COLORS}",
        cwd='/opt/airflow/plugins/dbt_project',
        env={
            "DBT_PROFILES_DIR": "."
        }
    )
