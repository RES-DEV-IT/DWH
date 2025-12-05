from pyairtable import Api
from typing import List
import os
import base64
from email.message import EmailMessage
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import yaml


# AIRTABLE
API_KEY = 'patObXahxyWNVxiCp.f5d54cb02ecd140b9c69d8494d51fd0cbf8bb113673e82245e3c426f0076ac2f'
BASE_ID = "appOHnOuN3MFA7VM3"

# GOOGLE
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
CREDS_PATH = "/".join(__file__.replace("\\", "/").split("/")[:-1]) + "/client_secret_19656404523-idm8shvv74tdk0qmoo0k93de2t9vfqvj.apps.googleusercontent.com.json"
GOOGLE_TOKEN_FOR_MAILING = "/".join(__file__.replace("\\", "/").split("/")[:-1]) + '/google_token_for_mailing.json'

# CONFIG
PATH_TO_CONFIG = "/".join(__file__.replace("\\", "/").split("/")[:-2]) + "/CONFIG.yaml"


class mailSender:
    def __init__(self,
                 airtable_api: Api,
                 bases_info: List[tuple[str, str, List[str]]],
                 testing_mode: bool = True) -> None:

        self.__testing_mode = testing_mode

        # Загружаем конфиг
        with open(PATH_TO_CONFIG, 'r', encoding='utf-8') as file:
            self.config = yaml.safe_load(file)

        self.__airtable_tables = []

        # Подключаемся ко всем базам с переносами
        for base_info in bases_info:
            # Получаем информацию о каждой таблице переносов
            base_id = base_info["base_id"]
            table_name = base_info["table_name"]
            manufs = base_info["manufs"]
            
            # Сохраняем таблицу
            self.__airtable_tables.append({
                "api": airtable_api.table(base_id, table_name),
                "manufs": manufs
            })
        
        # Подключаемся к базам для поиска имен инженеров
        self.projects_data = airtable_api.table(BASE_ID, "Projects").all(fields=["Project No.", "Email (ICP)"])
        self.res_responsible_persons_data = airtable_api.table(BASE_ID, "RES responsible persons").all(fields=["Name", "Email"])

        # Создаём сервис гугла для отправки писем
        self.creds = None

        if os.path.exists(GOOGLE_TOKEN_FOR_MAILING):
            self.creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_FOR_MAILING, SCOPES)
            
        # Если нет валидных учетных данных, запросите у пользователя войти
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CREDS_PATH, SCOPES)
                self.creds = flow.run_local_server(port=0)
    
        # Сохраните учетные данные для следующего запуска
        with open(GOOGLE_TOKEN_FOR_MAILING, 'w') as token:
            token.write(self.creds.to_json())
    
    def new_shifts_by_manuf_name(self, manuf_name: str):
        working_table = None

        # Находим рабочуюю таблицу
        for table in self.__airtable_tables:
            if manuf_name in table["manufs"]:
                working_table = table
                break
        
        # Если таблица не найдена, то выходим с ошибкой
        if working_table is None:
            raise f"Can't find table for your manuf: {manuf_name}"
        
        # Выгружаем все TODAY записи для конкретного завода и не mailed
        if manuf_name != "Jsons":
            not_mailed_records = working_table["api"].all(view="TODAY-SHIFTS-ALL", formula=f"AND({{Manufacturer}}='{manuf_name}', {{mailed ICP}}=FALSE())")
        else:
            not_mailed_records = working_table["api"].all(view="TODAY-SHIFTS-ALL", formula="{mailed ICP}=FALSE()")
        return not_mailed_records, working_table["api"]
    
    def change_date_format(self, date: str) -> str:
        # 2025-05-26 ---> 26.05.2025

        year, month, day = date.split("-")
        if len(month) == 1:
            month = "0" + month
        if len(day) == 1:
            day = "0" + day
        
        return f"{day}.{month}.{year}"

    def engineer_name_by_sheet_name(self, sheet_name: str, manuf_name: str):

        # Получаем короткое имя завода
        short_manuf_name = self.config["SHORT_NAMES"][manuf_name]

        # Удаляем скобки
        sheet_name_no_brackets = sheet_name.split("(")[0].strip()

        # Находим как выглядит project_name (111-2 ---> 111-DL-2)
        spl = sheet_name_no_brackets.split("-")

        if len(spl) == 1:
            project_name = spl[0] + "-" + short_manuf_name
        else:
            if spl[-1].isdigit():
                if int(spl[-1]) < 10: # P138-139-1 ---> P138-139-DL-1
                    project_name = "-".join(spl[:-1]) + "-" + short_manuf_name + "-" + spl[-1]
                else: # P138-139-11 ---> P138-139-11-DL
                    project_name = "-".join(spl) + "-" + short_manuf_name
            else: # P138-139-11 ---> P138-139-11-DL
                project_name = "-".join(spl) + "-" + short_manuf_name
        # Находим проект в таблице Projects (с портала)
        projects = [project["fields"]["Email (ICP)"] for project in self.projects_data if project["fields"].get("Project No.", "").find(project_name) != -1]
        
        assert len(projects) == 1, f"len(projects) != 1 ---> project_name: {project_name}, pojects: {projects}"

        # Выбираем только первого человека из списка ответсвенных за проект
        email_of_responsible_person = projects[0][0]

        # Находим имя человека
        names = [name["fields"]["Name"] for name in self.res_responsible_persons_data if name["fields"].get("Email") == email_of_responsible_person]
        
        assert len(names) == 1, f"len(names) != 1 ---> email_of_responsible_person: {email_of_responsible_person}, project_name: {project_name}"
        
        return names[0], email_of_responsible_person
    
    def send_mail(self, airtable_records, manuf_name):
        
        # Если отправлять нечего, то сразу выходим из функции со статусом False
        if len(airtable_records) == 0:
            print("--->", manuf_name, "no shifts")
            return False, []
        
        # Получаем дату сообщения (должна быть равна TODAY)
        date = self.change_date_format(airtable_records[0]["fields"]["Date of submission"])

        # Парсим входящие records
        record_ids = []  # Собираем ids чтобы потом обновить в AirTable поле "mailed ICP"
        lines_by_pono = {}
        emails = set(["letyagin.a@res-e.ru"]) # Адреса для отправки, Артём всегда в копии
        for record in airtable_records:
        
            record_ids.append(record["id"])
            pono = record["fields"]["PO No."]

            # 
            if manuf_name == "Jsons":
                engineer_name =  "Anastasia Odrinskaya"
                engineer_email = "odrinskaya@res-e.ru"
            else:
                engineer_name, engineer_email = self.engineer_name_by_sheet_name(
                    sheet_name=record["fields"]["Sheet name"],
                    manuf_name=manuf_name
                )

            emails.add(engineer_email)

            if engineer_name not in lines_by_pono:
                lines_by_pono[engineer_name] = {}

            if pono not in lines_by_pono[engineer_name]:
                lines_by_pono[engineer_name][pono] = []

            lines_by_pono[engineer_name][pono].append({
                "items": ", ".join(record["fields"]["PO. ITEM"]),
                "shift_n_days": str(record["fields"]["Shift of task end, days"]) + " дней",
                "stage": ", ".join(record["fields"]["STAGE of shift (RU)"]),
                "readiness_to_inspection": record["fields"].get("READINESS to RES INSPECTION", "")
                # "sheet_name": record["fields"]["Sheet name"],
            })
        
        # Строим контент сообщения
        content = f"""<html>
                        <body>
                            <h1> Найдены переносы - {manuf_name}</h1>
                            <h2>Дата переноса: {date}</h2>
                            $LINES_CONTENT$
                        </body>
                    </html>"""
        
        lines_content = ""
        for engineer_name in lines_by_pono:
            lines_content += f"<h2>{engineer_name}</h2>\n"

            for pono in lines_by_pono[engineer_name]:
                lines_content += f"<h3 style='margin-left: 10px'>{pono}</h3>\n"
                for i, line in enumerate(lines_by_pono[engineer_name][pono]):
                    readiness_to_res_inspection = self.change_date_format(line['readiness_to_inspection'])
                    lines_content += f"<p style='margin-left: 20px'>{i + 1}) <b>Items:</b> {line['items']}, <b>Сдвиг на:</b> {line['shift_n_days']}, <b>Стадия:</b> {line['stage']}, <b>Дата готовности к инспекции:</b> {readiness_to_res_inspection}</p>\n"
         
        content = content.replace("$LINES_CONTENT$", lines_content)
        # Создаем Gmail API клиент
        service = build('gmail', 'v1', credentials=self.creds)

        # Создаем email сообщение
        message = EmailMessage()

        message.add_alternative(content, subtype='html')
        if not self.__testing_mode:
            message['To'] = list(emails)
        else:
            print("EMAILS in non test mode:", emails)
            message['To'] = ["letyagin.a@res-e.ru"]
        message['From'] = 'letyagin.a@res-e.ru'
        message['Subject'] = f'Реестр переносов - {manuf_name}'

        # Кодируем сообщение
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {'raw': encoded_message}

        # Отправляем сообщение
        send_message = service.users().messages().send(
            userId="me", body=create_message).execute()
        print("--->", manuf_name, "message sended")
        return True, record_ids
    
if __name__ == "__main__":
    # AIRTABLE API
    airtable_api = Api(API_KEY)
    bases_info = [
        {
            "base_id": "appOHnOuN3MFA7VM3",
            "table_name": "⏩Shifts",
            "manufs": ["Dembla", "LC", "RKC"]
        },
        {
            "base_id": "appOHnOuN3MFA7VM3",
            "table_name": "⏩Shifts Foundry",
            "manufs": ["Jsons"]
        },
    ]


    ms = mailSender(
        airtable_api=airtable_api,
        bases_info=bases_info,
        testing_mode=True
    )
    
    for manuf_name in ["Dembla", "RKC", "LC"]:
        records, table = ms.new_shifts_by_manuf_name(manuf_name=manuf_name)
        status, updated_records = ms.send_mail(
            airtable_records=records,
            manuf_name=manuf_name
        )

