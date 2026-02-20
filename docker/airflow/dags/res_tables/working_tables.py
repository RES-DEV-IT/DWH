from gspread import service_account
import pandas as pd
import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
from airflow.decorators import task


SERVICE_ACCOUNT_CREDS_PATH = "./plugins/schedules/download/submitted-tables-download-v02-750e825a7950.json"
# SERVICE_ACCOUNT_CREDS_PATH = "src/schedules/download/submitted-tables-download-v02-750e825a7950.json"
URL = "https://docs.google.com/spreadsheets/d/1QJfmmjdQn2UgAQWHT6Y-KvxwfzRuT1f_M1qUarVNWdQ/edit?clckid=4a56206c"

TARGET_COLUMNS = {
    "Проект": "project",
    "Приоритет": "priority",
    "Заказчик и куратор": "customer_and_supervisor",
    "Заказчик для РЭС": "customer_for_res",
    "Тип проекта": "project_type",
    "Предмет договора": "subject_of_the_agreement",
    "Объект поставки": "delivery_object",
    "Номер ген.договора": "general_agreement_number",
    "Сроки поставки": "delivery_time",
    "КБ / QA": "qa", 
    "Замены ЗИ / базиса": "basis",
    "Лид, ЛОТ": "lot",
    "Ключевое событие АЭС": "key_event",
    "Кураторы RES": "curators_res",
    "Со стороны EXE": "from_exe",
    "Цена ген. договора (с НДС)": "price_nds",
    "Цена ген. договора (без НДС)": "price_no_nds"
}

@task
def main():
    client = service_account(SERVICE_ACCOUNT_CREDS_PATH)
    worksheet = client.open_by_url(URL).worksheet("Проекты")

    values = worksheet.get_all_values()

    head = values[0]
    head = [TARGET_COLUMNS[col] for col in head]
    data = values[1:]

    # df = pd.DataFrame(data=data, columns=head)
    # df["_created_at"] = datetime.datetime.now()

    now = datetime.datetime.now()

    data_to_insert = [[now] + row for row in data]


    # Загружаем данные в PG
    hook = PostgresHook(postgres_conn_id="resdb_connection")
    
    conn = hook.get_conn()
    cursor = conn.cursor()
        
    # SQL запрос для вставки
    values_query = f"""
    INSERT INTO res_tables.working_tables (_created_at, project, priority, customer_and_supervisor, customer_for_res,
        project_type, subject_of_the_agreement, delivery_object, general_agreement_number, delivery_time,
        qa, basis, lot, key_event, curators_res, from_exe, price_nds, price_no_nds)
    VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s)
    """

    # Массовая вставка
    cursor.executemany(values_query, data_to_insert)

    conn.commit()
    cursor.close()
    conn.close()


CRON_EXP = "0 6-15/2 * * mon-fri"
START_DATE = datetime.datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "DEV",
    "retries": 0
}

with DAG(
    dag_id=f"working_tables",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    main()

