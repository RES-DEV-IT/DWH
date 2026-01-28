from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime
from airflow.decorators import task
from pyairtable import Table, Api

# API TOKENS
API_TOKENS = {
    "Portal": "patpHD9iVIlsFGE2w.80f76907cb8bdc1dffd465c3eb3f275bc26a7e247727bdd5559b07decc0eb7d9"
}

def get_table(
        token_name: str,
        base_name: str,
        table_name: str
) -> Table:
    """
    Функция для получения AirTable таблицы по её base_name и имени
    """

    # AIRTABLE API
    api_key = API_TOKENS[token_name]
    api = Api(api_key)
    available_bases = api.bases()

    for available_base in available_bases:
        if available_base.name == base_name:
            at_table = api.table(available_base.id, table_name)

            return at_table

    raise ValueError(f"Can't find base with name: {base_name}")

@task
def main_task():
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    # === Получаем актуальные записи ===
    records = hook.get_records("""
        SELECT 
            _manuf,
            _sheet_name,
            jsonb_array_elements(content) as content
        FROM (
            SELECT *, MAX(_created_at) OVER(PARTITION BY _manuf, _sheet_name) AS max_created_at
            FROM stage.schedules_values
        ) as t1
        WHERE _created_at = max_created_at        
    """)

    # === Получаем все возможные колонки ===
    unique_columns = set(["_manuf", "_sheet_name"])
    for r in records:
        unique_columns.update(r[2].keys())

    # === Обновляем колонки в airtable ===
    table = get_table("Portal", "Portal 2.0: Schedules DLC", "MasterSchedule_new")
    existing_columns = [field.name for field in table.schema().fields]

    for unique_column in list(unique_columns):
        if unique_column not in existing_columns:
            table.create_field(unique_column, field_type="singleLineText")

    # === Добавляем данные ===
    at_records = [r[2] for r in records] # extract content from each row
    table.batch_create(at_records)
    
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
