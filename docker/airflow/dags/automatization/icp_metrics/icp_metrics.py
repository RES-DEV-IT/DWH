from gspread import service_account
import pandas as pd
import yaml
from airflow.decorators import task
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import base64
from io import BytesIO
from email.message import EmailMessage
from send_file_by_mail import create_service
from typing import List, Dict


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
    if len(records) == 0:
        return None
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "manuf": r[0],
        "sheet_name": r[1],
        "po_item": r[2],
        "kks_num": r[3],
        "qty": r[4]
    } for r in records])
    return df

def empty_in_column(hook):
    QUERY = """
with key_values_schedules as (
    select 
        _manuf, 
        _sheet_name,
        --jsonb_array_elements(content) ->> '_row_number' as rn,
        kv.key,
        kv.value
    from (select *, max(_created_at) over(partition by _manuf, _sheet_name) max_created_at from stage.schedules_values) as t1,
    lateral jsonb_array_elements(content) as elem,
    lateral jsonb_each_text((elem - 'comments')::jsonb) as kv
    where _created_at = max_created_at
)
select key as column_name, count(*) as empty_count
from key_values_schedules
where value = ''
group by key
order by empty_count desc
"""
    records = hook.get_records(QUERY)
    if len(records) == 0:
        return None
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "column_name": r[0],
        "empty_count": r[1]
    } for r in records])
    return df

def build_html_summary(dfs_meta: List[Dict]) -> str:
    """
    dfs_meta: [{"title": "...", "rows": 123, "cols": 5, "columns": [...]}]
    """
    rows_html = []
    for m in dfs_meta:
        cols_preview = ", ".join(m["columns"][:12])
        if len(m["columns"]) > 12:
            cols_preview += ", …"

        rows_html.append(f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd;"><b>{m["title"]}</b></td>
          <td style="padding:8px;border:1px solid #ddd;">{m["rows"]}</td>
          <td style="padding:8px;border:1px solid #ddd;">{m["cols"]}</td>
          <td style="padding:8px;border:1px solid #ddd;">{cols_preview}</td>
        </tr>
        """)

    return f"""
    <html>
      <body style="font-family:Arial, sans-serif; font-size:14px; color:#111;">
        <p>Привет! Ниже — краткая сводка по вложенным таблицам. Во вложении Excel с отдельными листами.</p>

        <table style="border-collapse:collapse;">
          <thead>
            <tr>
              <th style="padding:8px;border:1px solid #ddd; text-align:left;">Таблица</th>
              <th style="padding:8px;border:1px solid #ddd; text-align:left;">Строк</th>
              <th style="padding:8px;border:1px solid #ddd; text-align:left;">Столбцов</th>
              <th style="padding:8px;border:1px solid #ddd; text-align:left;">Колонки (preview)</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>

        <p style="margin-top:16px;">Если нужно добавить/изменить метрики в письме — скажите какие именно.</p>
      </body>
    </html>
    """

def dataframes_to_excel_bytes(dfs: Dict[str, pd.DataFrame]) -> bytes:
    """
    dfs: {"SheetName": df, ...}
    Возвращает байты .xlsx файла.
    """
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for sheet_name, df in dfs.items():
            # Excel ограничивает имя листа 31 символом
            safe_name = str(sheet_name)[:31] or "Sheet"
            df.to_excel(writer, sheet_name=safe_name, index=False)
    bio.seek(0)
    return bio.read()

def send_email_with_html_and_excel(service, to_email, subject, dfs: Dict[str, pd.DataFrame], excel_filename="metrics.xlsx"):
    """
    dfs: {"Title/SheetName": df, ...}
    """
    # 1) метаданные для HTML
    dfs_meta = []
    for title, df in dfs.items():
        dfs_meta.append({
            "title": title,
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "columns": list(map(str, df.columns.tolist())),
        })

    # 2) HTML тело
    html_body = build_html_summary(dfs_meta)

    # 3) Excel во вложение
    excel_bytes = dataframes_to_excel_bytes(dfs)

    message = EmailMessage()
    message["To"] = to_email
    message["From"] = "me"
    message["Subject"] = subject

    # HTML
    message.set_content(html_body, subtype="html")

    # Вложение .xlsx
    message.add_attachment(
        excel_bytes,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=excel_filename
    )

    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    create_message = {"raw": encoded_message}

    return service.users().messages().send(userId="me", body=create_message).execute()

@task
def main_task():

    # === Подключаемся к БД ===
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    # === Получаем данные и закрываем подключение ===
    conn = hook.get_conn()
    
    df1 = kks_vs_qty(hook=hook)
    df2 = empty_in_column(hook=hook)
    dfs = {
        "KKS vs QTY": df1, 
        "EMPTY COUNTER": df2
    }

    conn.close()

    # === Формируем контент письма ===
    service = create_service()
    send_email_with_html_and_excel(
        service=service,
        to_email=["zavodovskaya@res-e.ru"],
        subject="Метрики для ICP",
        dfs=dfs,
        excel_filename="metrics.xlsx"
    )
CRON_EXP = None
START_DATE = datetime(2025, 11, 27, 4, 0, 0, 0)

default_args = {
    "owner": "DEV",
    "retries": 0
}

with DAG(
    dag_id="icp_metrics",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dags:
    
    main_task(
    )