from gspread import service_account
from pyairtable import Table, Api

SERVICE_ACCOUNT_CREDS_PATH = "./src/schedules/download/submitted-tables-download-v02-750e825a7950.json"
LEADS_URL = "https://docs.google.com/spreadsheets/d/1wbIJEoLoMxag7TBThrn6sPGPgg_t7-qbeauv2SdXoxU"

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


def main_task():

    # === Получаем AirTable таблицу ===
    leads_table = get_table("Portal", "Таблица лидов", "Лиды")

    # === Определяем поля ===
    fields=[
        "Номер лида", "Этап", "Объект поставки", "Энергоблоки", "Тип", "№ лота",
        "Наименование оборудования", "КБ", "Кол-во", "НМЦ / ЦД (сумма)",
        "Валюта (НМЦ / ЦД)", "Цена ТКП (сумма)"]

    # === Выгружаем строки с лидами AE ===
    rows = leads_table.all(
        fields=fields,
        formula="{RES / AE} = 'AE'",
        cell_format="string",
        time_zone="Europe/Moscow",  # Укажите ваш часовой пояс
        user_locale="ru"  # Укажите вашу локаль
    )

    # === Парсим строки и формируем батч ===
    data_to_insert = []
    for row in rows:
        row_as_dict = {}
        for field in fields:
            row_as_dict[field] = row["fields"].get(field, "")

        data_to_insert.append(row_as_dict)

    # === Сортируем строки по номеру лида ===
    data_to_insert = sorted(data_to_insert, key=lambda x: x["Номер лида"])
    data_to_insert = [list(d.values()) for d in data_to_insert]

    data_to_insert = [fields] + data_to_insert
    return data_to_insert

    # === Производим загрузку в google sheets ===
    print("Loading in google sheets...")
    

if __name__ == "__main__":

    data_to_insert = main_task()

    client = service_account(SERVICE_ACCOUNT_CREDS_PATH)

    worksheet = client.open_by_url(LEADS_URL).worksheet("Лиды AE")
    # client.open_by_url(LEADS_URL).add_worksheet(title="New sheet", rows=100, cols=20)

    #result_data = [[1, 2], [3, 4]]

    row_start = 1
    col_start = 1

    worksheet.update(
        f"R1C1:R{row_start + len(data_to_insert) - 1}C{col_start + len(data_to_insert[0]) - 1}",
        data_to_insert,
        raw=False
    )

