import re
from pyairtable import Table, Api
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
import datetime


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
def fetch_projects_table():
    projects_table = get_table("Portal", "Portal 2.0.3", "Projects")
    
    rows = projects_table.all(
        fields=["PO No.", "Manufacturer"],
        cell_format="string",
        time_zone="Europe/Moscow",  # Укажите ваш часовой пояс
        user_locale="ru"  # Укажите вашу локаль
    )

    data_to_insert = []

    for row in rows:
        if "PO No." in row["fields"] and "Manufacturer" in row["fields"]:
            data_to_insert.append((
                row["fields"]["PO No."],
                row["fields"]["Manufacturer"]
            ))

    # Загружаем данные в PG
    hook = PostgresHook(postgres_conn_id="resdb_connection")
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE portal.projects")
    cursor.commit()

    # SQL запрос для вставки
    insert_query = f"""
    INSERT INTO portal.projects (pono, manuf)
    VALUES (%s, %s)
    """
    
    # Массовая вставка
    cursor.executemany(data_to_insert, insert_query)
    
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    "owner": "Artem",
    "retries": 0
}

with DAG(
    dag_id=f"portal_projects",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval="0 0 * * *",
    catchup=False
) as dags:
    fetch_projects_table()
