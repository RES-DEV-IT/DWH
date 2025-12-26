from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
import datetime
from at_api import get_table


QUERY = """
select
    _created_at,
    s.project,
    concat(s.project, ' - ', _manuf),
    po_item,
    qty,
    kks,
    ps.process_en,
    ps.process_ru,
    try_parse_date(old_value) as before,
    try_parse_date(current_value) as after,
    _manuf
from schedules_changes as s
left join gold.project_stages as ps on s.key = ps.stage_name 
where try_parse_date(old_value) is not null
  and try_parse_date(current_value) is not null 
  and try_parse_date(old_value) != try_parse_date(current_value)
"""


@task
def main():
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    conn = hook.get_conn()
    cursor = conn.cursor()

    records = hook.get_records(QUERY)
    cursor.close()
    conn.close()

    at_records = []
    for row in records:
        at_records.append({
            "Date of submission": row[0],
            "PO No.": row[1],
            "PO No. - Manufacturer": row[2],
            "PO. ITEM": row[3],
            "QTY of valves": row[4],
            "KKS": row[5],
            "STAGE of shift": row[6],
            "STAGE of shift (RU)": row[7],
            "BEFORE (finish date)": row[8],
            "AFTER (finish date)": row[9]
        })

    at_table = get_table("Portal", "Portal 2.0.3", "⏩Shifts")
    at_table.insert(at_records)


CRON_EXP = None
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "Artem",
    "retries": 0
}

with DAG(
    dag_id="shifts",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    main()
    