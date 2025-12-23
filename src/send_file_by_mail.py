from email.message import EmailMessage
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import os
import base64
from googleapiclient.errors import HttpError


# GOOGLE
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
CREDS_PATH = "/".join(__file__.replace("\\", "/").split("/")[:-1]) + "/schedules/download/client_secret_19656404523_idm8shvv74tdk0qmoo0k93de2t9vfqvj_apps.json"
GOOGLE_TOKEN_FOR_MAILING = "/".join(__file__.replace("\\", "/").split("/")[:-1]) + '/schedules/download/google_token_for_mailing.json'


def create_service():
    creds = None
    if os.path.exists(GOOGLE_TOKEN_FOR_MAILING):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_FOR_MAILING, SCOPES)
    # Если нет валидных учетных данных, запросите у пользователя войти
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

    # Сохраните учетные данные для следующего запуска
    # with open(GOOGLE_TOKEN_FOR_MAILING, 'w') as token:
    #     token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    return service

def send_email_with_excel(service, to_email, subject, body, excel_file_path):
    """
    Отправляет email с Excel вложением
    
    Args:
        service: сервис Gmail API
        to_email: email получателя
        subject: тема письма
        body: текст письма
        excel_file_path: путь к Excel файлу
    """
    
    # Создаем email сообщение
    message = EmailMessage()
    
    # Заполняем заголовки
    message['To'] = to_email
    message['From'] = 'me'  # 'me' означает авторизованного пользователя
    message['Subject'] = subject
    
    # Устанавливаем текст письма
    message.set_content(body)
    
    # Читаем Excel файл как бинарные данные
    with open(excel_file_path, 'rb') as file:
        excel_data = file.read()
        file_name = os.path.basename(excel_file_path)
    
    # Определяем MIME тип для Excel файла
    # Для .xlsx используйте: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    # Для .xls используйте: 'application/vnd.ms-excel'
    if excel_file_path.endswith('.xlsx'):
        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif excel_file_path.endswith('.xls'):
        mime_type = 'application/vnd.ms-excel'
    else:
        mime_type = 'application/octet-stream'
    
    # Добавляем вложение
    message.add_attachment(
        excel_data,
        maintype='application',
        subtype=mime_type.split('/')[1] if '/' in mime_type else 'octet-stream',
        filename=file_name
    )
    
    # Кодируем сообщение в base64
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    
    # Создаем тело запроса для API
    create_message = {'raw': encoded_message}
    
    try:
        # Отправляем сообщение
        send_message = service.users().messages().send(
            userId="me",
            body=create_message
        ).execute()
        
        print(f'Email отправлен! Message Id: {send_message["id"]}')
        return send_message
    
    except HttpError as error:
        print(f'Произошла ошибка: {error}')
        return None

def send_email_with_text(service, to_email, subject, body):
    # Создаем email сообщение
    message = EmailMessage()
    
    # Заполняем заголовки
    message['To'] = to_email
    message['From'] = 'me'  # 'me' означает авторизованного пользователя
    message['Subject'] = subject

    # Устанавливаем текст письма
    message.set_content(body)

    # Кодируем сообщение в base64
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    
    # Создаем тело запроса для API
    create_message = {'raw': encoded_message}
    
    try:
        # Отправляем сообщение
        send_message = service.users().messages().send(
            userId="me",
            body=create_message
        ).execute()
        
        print(f'Email отправлен! Message Id: {send_message["id"]}')
        return send_message
    
    except HttpError as error:
        print(f'Произошла ошибка: {error}')
        return None

def send_email_with_html(service, to_email, subject, html_body):
    message = EmailMessage()
    message["To"] = to_email
    message["From"] = "me"
    message["Subject"] = subject

    # ❗ ВАЖНО: subtype="html"
    message.set_content(html_body, subtype="html")

    encoded_message = base64.urlsafe_b64encode(
        message.as_bytes()
    ).decode("utf-8")

    create_message = {"raw": encoded_message}

    return service.users().messages().send(
        userId="me",
        body=create_message
    ).execute()

if __name__ == "__main__":

    # === SEND EXCEL ===
    # service = create_service()
    # to_email = "letyagin.a@res-e.ru"
    # subject = "test_subject"
    # body = "test_body"
    # excel_file_path = "tmp/Dembla.xlsx"
    # send_email_with_excel(service, to_email, subject, body, excel_file_path)

    # === SEND TEXT ===
    service = create_service()
    to_email = "letyagin.a@res-e.ru"
    subject = "test_subject"
    body = "main text"
    send_email_with_text(service, to_email, subject, body)

