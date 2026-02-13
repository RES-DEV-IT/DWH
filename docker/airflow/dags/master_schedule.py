from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime
from airflow.decorators import task
from pyairtable import Table, Api
import re
from at_api import get_table, get_table_for_field_create


def normalize_string(s: str) -> str:
    # 1) Оставляем только латиницу, цифры и пробелы
    s = re.sub(r'[^A-Za-z0-9 _]+', '', s)

    # 2) Сжимаем последовательности пробелов в один пробел
    s = re.sub(r'\s+', ' ', s).strip()

    # 3) Меняем пробелы на подчёркивания
    s = s.replace(' ', '_')

    # 4) В нижний регистр
    return s.lower()

@task
def main_task():
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    # === Получаем актуальные записи ===
    records = hook.get_records("""
        SELECT 
            _created_at,
            _manuf,
            _sheet_name,
            CONCAT(_manuf, '_', _sheet_name) as _sheet_name_manuf,
            array_agg(concat(_manuf, '_', _sheet_name, '_', po_item, '_', kks)) as kks_new_link,
            string_agg(concat(_manuf, '_', _sheet_name, '_', po_item, '_', kks), ', ') as _unique_field,
            content
        FROM (              
            SELECT 
                _created_at,
                _manuf,
                _sheet_name,
                REPLACE(po_item, ',', '.') as po_item,
                unnest(string_to_array(kks, E'\n')) as kks,
                content
            FROM (
                SELECT 
                    TO_CHAR(_created_at, 'YYYY-MM-DD') as _created_at,
                    _manuf,
                    _sheet_name,
                    jsonb_array_elements(content) ->> 'po_item' as po_item,
                    jsonb_array_elements(content) ->> 'kks' as kks,
                    jsonb_array_elements(content) as content
                FROM (
                    SELECT *, MAX(_created_at) OVER(PARTITION BY _manuf, _sheet_name) AS max_created_at
                    FROM stage.schedules_values
                ) as t1
                WHERE _created_at = max_created_at 
                AND _created_at > CURRENT_TIMESTAMP - interval '1 day'
                --LIMIT 10
            ) AS t1
        ) AS t2
        group by _created_at, _manuf, _sheet_name, CONCAT(_manuf, '_', _sheet_name), content
    """)

    # === Получаем все возможные колонки ===
    unique_columns = set(["_created_at", "_manuf", "_sheet_name", "kks_new_link", "_unique_field", "_sheet_name_manuf"])

    
    for r in records:
        fields_from_pg = r[-1].keys()
        fields_from_pg_normalized = [normalize_string(field) for field in fields_from_pg]
        unique_columns.update(fields_from_pg_normalized)

    # === Обновляем колонки в airtable ===
    table = get_table("Portal", "Portal 2.0: Schedules DLC", "MasterSchedule_new")
    table_for_field_create = get_table_for_field_create("Portal", "Portal 2.0: Schedules DLC", "MasterSchedule_new")
    existing_columns = [field.name for field in table.schema().fields]

    print("UNIQUE COLUMNS", unique_columns)
    for unique_column in list(unique_columns):
        if unique_column not in existing_columns:
            print("TRYING TO CREATE", unique_column)
            # table.create_field(unique_column, field_type="singleLineText")
            table_for_field_create.create_field(unique_column, field_type="multilineText")

    # === Преобразуем данные в формат AirTable ===
    at_records = []
    for record in records:
        at_record = record[-1].copy() # content
        at_record["_created_at"] = record[0]
        at_record["_manuf"] = record[1]
        at_record["_sheet_name"] = record[2]
        at_record["_sheet_name_manuf"] = record[3]
        at_record["kks_new_link"] = record[4]
        at_record["_unique_field"] = record[5]
        at_records.append(at_record)

    at_records_for_upsert = [{"fields": r} for r in at_records]
    # === Добавляем данные ===
    # table.batch_create(at_records, typecast=True)
    for r in at_records_for_upsert:
        try:
            table.batch_upsert([r], key_fields=["_unique_field"], typecast=True)
        except Exception as e:
            print("EEEEEEEEEEEE", e, r, type(r))
    
    # for r in at_records:
    #     try:
    #         table.batch_create([r], typecast=True)
    #     except Exception as e:
    #         print("EEEEEEEEEEEE", e, r, type(r))

with DAG(
    dag_id="master_schedule",
    default_args={"owner": "Artem", "retries": 0},
    start_date=datetime.datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval=None,
    catchup=False
) as dags:
    main_task()

# if __name__ == "__main__":
#     table = get_table("Portal", "Portal 2.0: Schedules DLC", "MasterSchedule_new")
#     columns_names = [field.name for field in table.schema().fields]
#     print(columns_names)

#     table.create_field("111", field_type="singleLineText")
