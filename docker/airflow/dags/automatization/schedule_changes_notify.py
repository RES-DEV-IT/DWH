from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
import datetime
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from send_file_by_mail import create_service, send_email_with_html
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

import re

def _slugify(text: str) -> str:
    """Делаем безопасный id для HTML якоря."""
    if text is None:
        return "x"
    s = str(text).strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9а-яё\-_]+", "", s, flags=re.IGNORECASE)
    if not s:
        s = "x"
    return s[:60]  # чтобы id не был огромным

def for_content_to_html(for_content: dict, title: str = "Найдены переносы", max_changes_per_po: int = 5) -> str:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    def fmt_change(key, old, new):
        key = "" if key is None else str(key)
        old = "" if old is None else str(old)
        new = "" if new is None else str(new)

        return f"""
        <tr>
          <td style="padding:6px 10px; border-bottom:1px solid #eee;">
            <div style="font-size:14px; line-height:1.35;">
              <div style="font-weight:600;">{escape(key)}</div>
              <div style="color:#444; margin-top:2px;">
                <span style="color:#777;">{escape(old)}</span>
                <span style="color:#999;">&nbsp;→&nbsp;</span>
                <span style="color:#111; font-weight:600;">{escape(new)}</span>
              </div>
            </div>
          </td>
        </tr>
        """

    def fmt_more_row():
        return """
        <tr>
          <td style="padding:8px 10px; background:#fafafa; color:#555; font-size:13px;">
            <i>… и все следующие этапы</i>
          </td>
        </tr>
        """

    responsibles = sorted(for_content.keys(), key=lambda x: str(x).lower())
    blocks = []

    for responsible in responsibles:
        projects = for_content.get(responsible, {})
        project_names = sorted(projects.keys(), key=lambda x: str(x).lower())

        project_blocks = []
        project_details_blocks = []

        for idx, project in enumerate(project_names, start=1):
            changes = projects.get(project, []) or []

            # группировка по po_item
            grouped = defaultdict(list)
            for item in changes:
                if isinstance(item, (list, tuple)) and len(item) == 4:
                    po_item, key, old, new = item
                    grouped[po_item].append((key, old, new))

            if not grouped:
                continue

            # уникальные якоря
            proj_slug = _slugify(project)
            resp_slug = _slugify(responsible)
            anchor_top = f"p_{resp_slug}_{proj_slug}_{idx}"
            anchor_body = f"{anchor_top}_details"

            # Заголовок проекта (компактный)
            project_blocks.append(f"""
            <div id="{anchor_top}" style="margin-top:12px; padding:10px 12px; border:1px solid #eee; border-radius:12px;">
              <div style="display:block; font-size:15px; font-weight:800; margin-bottom:2px;">
                {escape(str(project))}
              </div>
              <div style="font-size:13px; color:#666;">
                <a href="#{anchor_body}" style="text-decoration:none; font-weight:700;">Показать</a>
              </div>
            </div>
            """)

            # Детали проекта (вынесены вниз, чтобы сверху было коротко)
            po_blocks = []
            for po_item in sorted(grouped.keys(), key=lambda x: str(x).lower()):
                po_changes = grouped[po_item]

                visible_changes = po_changes[:max_changes_per_po]
                rows_html = "".join(fmt_change(*c) for c in visible_changes)

                if len(po_changes) > max_changes_per_po:
                    rows_html += fmt_more_row()

                po_blocks.append(f"""
                <div style="margin-top:12px;">
                  <div style="font-size:14px; font-weight:700; color:#333; margin-bottom:6px;">
                    {escape(str(po_item))}
                  </div>
                  <table role="presentation" cellpadding="0" cellspacing="0" border="0"
                         style="width:100%; border:1px solid #eee; border-radius:10px; border-collapse:separate; overflow:hidden;">
                    {rows_html}
                  </table>
                </div>
                """)

            project_details_blocks.append(f"""
            <div id="{anchor_body}" style="margin-top:18px; padding:14px; border:1px solid #ddd; border-radius:14px; background:#fff;">
              <div style="font-size:16px; font-weight:900; margin-bottom:6px;">
                {escape(str(project))}
              </div>

              <div style="font-size:13px; color:#666; margin-bottom:10px;">
                <a href="#{anchor_top}" style="text-decoration:none; font-weight:700;">Скрыть</a>
              </div>

              {''.join(po_blocks)}

              <div style="margin-top:12px; font-size:13px; color:#666;">
                <a href="#{anchor_top}" style="text-decoration:none; font-weight:700;">↑ Скрыть и вернуться к списку проектов</a>
              </div>
            </div>
            """)

        if project_blocks:
            body_part = f"""
            <div style="margin-top:10px;">
              <div style="font-size:13px; color:#777; margin-bottom:8px;">
                Нажмите <b>Показать</b> у нужного проекта — детали откроются ниже.
              </div>
              {''.join(project_blocks)}
              <div style="margin-top:22px;">
                {''.join(project_details_blocks)}
              </div>
            </div>
            """
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

        {''.join(blocks)}

        <div style="margin-top:18px; font-size:12px; color:#888;">
          Это автоматическое уведомление.
        </div>
      </div>
    </div>
  </body>
</html>
"""
    return html


def ru_person_to_mail(ru_person):
    person_to_mail = {
        "Константин Исаев": "isaev@res-e.ru",
        "Дмитрий Пухов": "puhov@res-e.ru",
        "Пухов Дмитрий": "puhov@res-e.ru",
        "Виктория Шелудько": "sheludko@res-e.ru",
        "Анастасия Горбулина": "gorbulina@res-e.ru",
        "Ирина Пшенцова": "pshentsova@res-e.ru",
        "Андрей Бойко": "boyko@res-e.ru"
    }

    assert ru_person in person_to_mail, f"Can't find {ru_person} in base"

    return person_to_mail[ru_person]

@task
def fetch_changes():
    hook = PostgresHook(postgres_conn_id="resdb_connection")

    conn = hook.get_conn()
    cursor = conn.cursor()

    records = hook.get_records("""
        select s.project, po_item, key, try_parse_date(old_value), try_parse_date(current_value), responsible
        from schedules_changes as s
        left join project_responsible as pr
        on s.project = pr.project
        where try_parse_date(old_value) is not null
          and try_parse_date(current_value) is not null
          and try_parse_date(old_value) != try_parse_date(current_value)
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

    # === Готовим данные ===
    # for_content = {
    #  resp_1: {project: [po_item, key, old_value, current_value]},
    #  resp_2: {project: [po_item, key, old_value, current_value]}
    #  ...
    # }
    for_content = build_for_content(records=records)
    
    service = create_service()
    to_email = ["letyagin.a@res-e.ru"]#, "meyendorff@res-e.ru"]
    subject = "Переносы в графиках"
    
    for responsible in for_content:
        responsible_mail = ru_person_to_mail(responsible)

        html_body = for_content_to_html({responsible + f" ({responsible_mail})": for_content[responsible]})

        send_email_with_html(service, to_email, subject, html_body)

default_args = {
    "owner": "Artem",
    "retries": 0
}

with DAG(
    dag_id=f"schedule_changes",
    default_args=default_args,
    start_date=datetime.datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval="30 6-15/2 * * mon-fri",
    catchup=False
) as dags:
    fetch_changes()
