from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
import datetime


@task
def fetch_changes():
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    conn = hook.get_conn()
    cursor = conn.cursor()

    records = cursor.get_records("""
        select *
        from schedules_changes as s
        left join project_responsible as pr
        on s.project = pr.project
    """)

    for row in records:
        print(row)

    cursor.close()
    conn.close()

default_args = {
    "owner": "Artem",
    "retries": 0
}

with DAG(
    dag_id=f"schedule_changes",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval="0 0 * * *",
    catchup=False
) as dags:
    fetch_changes()
