from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime
from airflow.decorators import task
import re
from at_api import get_table


with DAG(
    dag_id="stage_kks_to_at",
    default_args={"owner": "DEV", "retries": 0},
    start_date=datetime.datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval="0 0 * * *",
    catchup=False
) as dags:
    
    @task
    def main_task():
        at_table = get_table("Portal", "Portal 2.0: Schedules DLC", "KKS_new")

        hook = PostgresHook(postgres_conn_id="resdb_connection")

        records = hook.get_records(
            """
            SELECT
                _created_at,
                _manuf,
                _sheet_name,
                po_item,
                kks,
                concat(_manuf, '_', _sheet_name, '_', po_item, '_', kks) as _unique_field
            FROM (
                SELECT
                    TO_CHAR(_created_at::DATE, 'YYYY-MM-DD') as _created_at,
                    _manuf,
                    _sheet_name,
                    REPLACE(po_item, ',', '.') AS po_item,
                    UNNEST(string_to_array(kks, E'\n')) AS kks
                FROM (
                    SELECT
                        _created_at, _manuf, _sheet_name,
                        jsonb_array_elements(content) ->> 'po_item' as po_item,
                        jsonb_array_elements(content) ->> 'kks' as kks
                    from (SELECT *, max(_created_at) over(partition by _manuf, _sheet_name) max_created_at from stage.schedules_values) as t1
                    where _created_at = max_created_at
                    and _created_at > CURRENT_TIMESTAMP - interval '3 day'
                ) AS t1
                -- WHERE po_item ~ '^\d+\.\d+$'
            ) AS t2
            """
        )

        
        print(f"\n\nRECORDS LEN {len(records)}\n\n")
        

        payload = []
        for created_at, manuf, sheet_name, po_item, kks, unique_field in records:
            payload.append({
                "fields": {
                    "_created_at": created_at,  # DATE
                    "_manuf": manuf,                              # single select
                    "_sheet_name": sheet_name,                    # single select
                    "po_item": po_item,                           # single select
                    "kks": kks,                                   # single line text
                    "_unique_field": unique_field
                }
            })

        res = at_table.batch_upsert(
            records=payload,
            key_fields=["_unique_field"],
            typecast=True,
        )
        return res
    
    main_task()

    