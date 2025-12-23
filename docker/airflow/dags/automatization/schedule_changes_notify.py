from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
import datetime
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from send_file_by_mail import create_service, send_email_with_text
from collections import defaultdict
from html import escape

def build_for_content(records):
    for_content = defaultdict(lambda: defaultdict(list))
    for row in records:
        for_content[row["responsible"]][row["project"]].append((
            row["po_item"],
            row["key"],
            row["old_value"],
            row["current_value"],
        ))
    return for_content

def for_content_to_html(for_content: dict, title: str = "Найдены переносы") -> str:
    """
    Делает HTML-письмо из структуры:
    {responsible: {project: [(po_item, key, old, new), ...]}}
    """

    # Базовые стили (инлайн/внутри <style> обычно ок в почте; но инлайн ещё надёжнее)
    # Здесь сделано максимально совместимо с почтовыми клиентами.
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    def fmt_change(po_item, key, old, new):
        po_item = "" if po_item is None else str(po_item)
        key = "" if key is None else str(key)
        old = "" if old is None else str(old)
        new = "" if new is None else str(new)

        # Вариант строки: "PO-123 • key1 — old → new"
        left = f"{escape(po_item)} • {escape(key)}" if po_item else escape(key)

        return f"""
        <tr>
          <td style="padding:6px 10px; border-bottom:1px solid #eee;">
            <div style="font-size:14px; line-height:1.35;">
              <div style="font-weight:600;">{left}</div>
              <div style="color:#444; margin-top:2px;">
                <span style="color:#777;">{escape(old)}</span>
                <span style="color:#999;">&nbsp;→&nbsp;</span>
                <span style="color:#111; font-weight:600;">{escape(new)}</span>
              </div>
            </div>
          </td>
        </tr>
        """

    # Сортировки для стабильного вида
    responsibles = sorted(for_content.keys(), key=lambda x: str(x).lower())

    # Если ты генеришь письмо "на каждого человека отдельно", то можно передавать
    # for_content уже для одного responsible.
    # Но функция умеет и много людей (будет блоками).
    blocks = []

    for responsible in responsibles:
        projects = for_content.get(responsible, {})
        project_names = sorted(projects.keys(), key=lambda x: str(x).lower())

        project_blocks = []
        for project in project_names:
            changes = projects.get(project, []) or []
            # На всякий случай: если кто-то положил не tuples
            normalized = []
            for item in changes:
                if isinstance(item, (list, tuple)) and len(item) == 4:
                    normalized.append(item)
                else:
                    # пропускаем некорректные элементы, чтобы не падать
                    continue

            # Если изменений нет — можно пропустить проект или показать пусто.
            if not normalized:
                continue

            rows_html = "".join(fmt_change(*c) for c in normalized)

            project_blocks.append(f"""
            <div style="margin-top:14px;">
              <div style="font-size:15px; font-weight:700; margin-bottom:6px;">
                {escape(str(project))}
              </div>
              <table role="presentation" cellpadding="0" cellspacing="0" border="0"
                     style="width:100%; border:1px solid #eee; border-radius:10px; border-collapse:separate; overflow:hidden;">
                {rows_html}
              </table>
            </div>
            """)

        # Если у человека нет блоков — можно вернуть "нет изменений"
        if project_blocks:
            body_part = "".join(project_blocks)
        else:
            body_part = """
            <div style="margin-top:10px; color:#666; font-size:14px;">
              Изменений не найдено.
            </div>
            """

        blocks.append(f"""
        <div style="margin-top:20px; padding:16px; border:1px solid #eee; border-radius:14px;">
          <div style="font-size:13px; color:#666; margin-bottom:6px;">
            Получатель: <span style="color:#111; font-weight:600;">{escape(str(responsible))}</span>
          </div>
          {body_part}
        </div>
        """)

    blocks_html = "".join(blocks)

    html = f"""\
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{escape(title)}</title>
  </head>
  <body style="margin:0; padding:0; background:#fafafa;">
    <div style="max-width:720px; margin:0 auto; padding:18px;">
      <div style="background:#ffffff; border:1px solid #eee; border-radius:16px; padding:18px;">
        <div style="font-size:18px; font-weight:800; margin:0 0 6px 0;">
          {escape(title)}
        </div>
        <div style="font-size:12px; color:#777; margin-bottom:14px;">
          Сформировано: {escape(now)}
        </div>

        {blocks_html}

        <div style="margin-top:18px; font-size:12px; color:#888;">
          Это автоматическое уведомление.
        </div>
      </div>
    </div>
  </body>
</html>
"""
    return html


@task
def fetch_changes():
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    conn = hook.get_conn()
    cursor = conn.cursor()

    records = hook.get_records("""
        select s.project, po_item, key, old_value, current_value, responsible
        from schedules_changes as s
        left join project_responsible as pr
        on s.project = pr.project
    """)

    cursor.close()
    conn.close()

    records = [{
        "project": row[0],
        "po_item": row[1],
        "key": row[2],
        "old_value": row[3],
        "current_value": row[4],
        "responsible": row[5]
        } 
    for row in records]

    # for_content = {
    #  resp_1: {project: [po_item, key, old_value, current_value]},
    #  resp_2: {project: [po_item, key, old_value, current_value]}
    #  ...
    # }
    for_content = build_for_content(records=records)
    
    service = create_service()
    to_email = "letyagin.a@res-e.ru"
    subject = "test_subject"
    
    for responsible in for_content:
        
        html_body = for_content_to_html({responsible: for_content[responsible]})

        send_email_with_text(service, to_email, subject, html_body)

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
