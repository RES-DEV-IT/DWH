from gspread import service_account
import pandas as pd
import yaml
from airflow.decorators import task
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

def kks_vs_qty(hook):
    KKS_VS_QTY_QUERY = """
-- === KKS vs QTY ===
select _manuf, _sheet_name, po_item,
  array_length(string_to_array(kks, E'\n'), 1) as kks_num,
  coalesce(qty_of_valves, qty_of_pumps) as qty
from (
  select 
    _manuf, _sheet_name,
    jsonb_array_elements(content) ->> 'po_item' as po_item,
    jsonb_array_elements(content) ->> 'kks' as kks,
    jsonb_array_elements(content) ->> 'qty_of_valves' as qty_of_valves,
    jsonb_array_elements(content) ->> 'qty_of_pumps' as qty_of_pumps
  from (select *, max(_created_at) over(partition by _manuf, _sheet_name) as max_created_at from stage.schedules_values) as t1
  where _created_at = max_created_at
) as t2
where array_length(string_to_array(kks, E'\n'), 1) != try_cast_int(coalesce(qty_of_valves, qty_of_pumps))
  and kks not like '%-%'
  and kks not like '%NA%'

"""
    records = hook.get_records(KKS_VS_QTY_QUERY)
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "manuf": r[0],
        "sheet_name": r[1],
        "po_item": r[2],
        "kks_num": r[3],
        "qty": r[4]
    } for r in records])
    return df

@task
def main_task():

    # === Подключаемся к БД ===
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    # === Получаем данные и закрываем подключение ===
    conn = hook.get_conn()
    
    df1 = kks_vs_qty(hook=hook)
    print(df1)

    conn.close()


CRON_EXP = None
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "Artem",
    "retries": 0
}

with DAG(
    dag_id="icp_metrics",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    
    main_task()