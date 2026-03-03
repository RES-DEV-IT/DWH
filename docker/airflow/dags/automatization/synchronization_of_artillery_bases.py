from datetime import datetime, timedelta
from airflow.decorators import task
from airflow import DAG
from pyairtable import Table, Api
from gspread import service_account

# ============ КОНФИГУРАЦИЯ ============

SERVICE_ACCOUNT_CREDS_PATH = "./plugins/schedules/download/excellent-ship-487111-n9-b20891397bd5.json"
a = ".././"
PROJECTS_URL = "https://docs.google.com/spreadsheets/d/1cBo4dY58Eic0EZwz5jhXR893nP-NfuKSd-0TEm9Zgsw/edit"

API_TOKENS = {
    "Portal": "patpHD9iVIlsFGE2w.80f76907cb8bdc1dffd465c3eb3f275bc26a7e247727bdd5559b07decc0eb7d9"  # Замени на свой токен
}

UNIQUE_FIELD = "ID арматуры"

SYNC_FIELDS = [
    "ID арматуры", "Обозначение", "Вид ТПА", "Тип ТПА", "Дизайн ЗИ", "ЗИ", "Проект",
    "ДУ", "Рр, МПа", "Максимальный перепад, МПа", "Тр, С", "Масса по ТЗ, кг",
    "Масса по FAT, кг", "Длина, мм", "Высота, мм", "Кву", "Соосность",
    "Расположение крышки", "Класс герметичности", "Категория сейсмостойкости",
    "Материал", "АЭС", "Классификация по НП-068-05", "КОК", "Класс давления",
    "Класс давления по чертежу ЗИ", "Тип регул. органа", "Тип управления",
    "Мощность двигателя, кВт", "Время открытия/ закрытия, с", "Обозначение привода",
    "Поставочный документ", "Вид проп хар", "Диаметр расточки", "Типоразмер трубы",
    "Место установки", "Тип среды", "Система", "Здание", "ККС", "Серия/ОО",
    "Чем закрывались ПИ", "Наименование ОО/ ГО", "Наименование ТЗ/ТУ", "Способ присоединения"
]

# ============ ФУНКЦИИ ============

def get_table(token_name: str, base_name: str, table_name: str) -> Table:
    api_key = API_TOKENS[token_name]
    api = Api(api_key)
    for available_base in api.bases():
        if available_base.name == base_name:
            return api.table(available_base.id, table_name)
    raise ValueError(f"Can't find base with name: {base_name}")

def get_gs_client():
    return service_account(SERVICE_ACCOUNT_CREDS_PATH)

def read_gs_data(worksheet_name="Projects Sync"):
    """Читаем данные из Google Sheets"""
    client = get_gs_client()
    try:
        worksheet = client.open_by_url(PROJECTS_URL).worksheet(worksheet_name)
        records = worksheet.get_all_records()
        return records
    except Exception as e:
        print(f"Ошибка чтения GS: {e}")
        return []

def read_airtable_data(table):
    """Читаем данные из Airtable"""
    records = table.all(
        fields=SYNC_FIELDS,
        cell_format="string",
        time_zone="Europe/Moscow",
        user_locale="ru"
    )
    data = {}
    for r in records:
        key = str(r["fields"].get(UNIQUE_FIELD, "")).strip()
        if key:
            data[key] = {
                "id": r["id"],
                "fields": r["fields"]
            }
    return data

# ============ ДВУСТОРОННЯЯ СИНХРОНИЗАЦИЯ ============

@task
def main_task():
    
    # === Подключаемся к источникам ===
    master_table = get_table("Portal", "База арматуры РЭС", "Мастер-таблица")
    
    # === Читаем данные из ОБОИХ систем ===
    print("Чтение из Airtable...")
    at_data = read_airtable_data(master_table)
    print(f"Записей в Airtable: {len(at_data)}")
    
    print("Чтение из Google Sheets...")
    gs_records = read_gs_data()
    print(f"Записей в Google Sheets: {len(gs_records)}")
    
    # === Анализируем различия ===
    gs_only = []      # Есть в GS, нет в AT (создать в AT)
    at_only = []      # Есть в AT, нет в GS (создать в GS)
    both_diff = []    # Есть в обоих, но различаются (обновить)
    both_same = 0     # Есть в обоих, одинаковые
    
    gs_map = {}
    for r in gs_records:
        key = str(r.get(UNIQUE_FIELD, "")).strip()
        if key:
            gs_map[key] = r
    
    # Проверяем что есть в GS
    for key, gs_row in gs_map.items():
        if key not in at_data:
            gs_only.append(gs_row)
        else:
            # Проверяем различия
            at_row = at_data[key]["fields"]
            has_diff = False
            
            for field in SYNC_FIELDS:
                gs_val = str(gs_row.get(field, "")).strip()
                at_val = str(at_row.get(field, "")).strip()
                if gs_val != at_val:
                    has_diff = True
                    break
            
            if has_diff:
                both_diff.append({
                    "key": key,
                    "gs": gs_row,
                    "at": at_data[key],
                    "at_id": at_data[key]["id"]
                })
            else:
                both_same += 1
    
    # Проверяем что есть в AT, но нет в GS
    for key, at_row in at_data.items():
        if key not in gs_map:
            at_only.append(at_row["fields"])
    
    print("=" * 50)
    print("АНАЛИЗ РАЗЛИЧИЙ")
    print(f"Только в Google Sheets (будут созданы в AT): {len(gs_only)}")
    print(f"Только в Airtable (будут созданы в GS): {len(at_only)}")
    print(f"Различаются (будут синхронизированы): {len(both_diff)}")
    print(f"Идентичны: {both_same}")
    
    # === СИНХРОНИЗАЦИЯ ===
    
    # 1. Создаем в Airtable записи из GS
    created_in_at = 0
    for row in gs_only:
        try:
            # Убираем пустые значения
            fields = {k: v for k, v in row.items() if v and k in SYNC_FIELDS}
            master_table.create(fields)
            created_in_at += 1
            print(f"Создано в AT: {row.get(UNIQUE_FIELD)}")
        except Exception as e:
            print(f"Ошибка создания в AT {row.get(UNIQUE_FIELD)}: {e}")
    
    # 2. Обновляем существующие (GS → AT)
    updated_in_at = 0
    for diff in both_diff:
        try:
            new_fields = {k: v for k, v in diff["gs"].items() if k in SYNC_FIELDS}
            master_table.update(diff["at_id"], new_fields)
            updated_in_at += 1
            print(f"Обновлено в AT: {diff['key']}")
        except Exception as e:
            print(f"Ошибка обновления в AT {diff['key']}: {e}")
    
    # 3. Перезаписываем Google Sheets полными данными из Airtable
    print("Обновление Google Sheets...")
    
    # Перечитываем AT (там теперь новые записи)
    at_data_full = read_airtable_data(master_table)
    
    # Формируем данные для GS
    gs_data = []
    for key in sorted(at_data_full.keys()):
        row = at_data_full[key]["fields"]
        gs_row = [row.get(f, "") for f in SYNC_FIELDS]
        gs_data.append(gs_row)
    
    # Записываем в GS
    client = get_gs_client()
    try:
        worksheet = client.open_by_url(PROJECTS_URL).worksheet("Projects Sync")
    except:
        worksheet = client.open_by_url(PROJECTS_URL).add_worksheet(
            title="Projects Sync", rows=10000, cols=60
        )
    
    worksheet.clear()
    worksheet.append_row(SYNC_FIELDS)
    
    if gs_data:
        worksheet.update(
            f"R2C1:R{len(gs_data)+1}C{len(SYNC_FIELDS)}",
            gs_data,
            raw=False
        )
    
    created_in_gs = len(at_only)
    
    print("=" * 50)
    print("СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА")
    print(f"Создано в Airtable: {created_in_at}")
    print(f"Обновлено в Airtable: {updated_in_at}")
    print(f"Создано в Google Sheets: {created_in_gs}")
    print(f"Итого записей: {len(gs_data)}")

# ============ DAG ============

CRON_EXP = "0 * * * *"  # Каждый час
START_DATE = datetime(2025, 1, 1, 4, 0, 0)

default_args = {
    "owner": "DEV",
    "retries": 0
}

with DAG(
    dag_id="master_table_sync",
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=CRON_EXP,
    catchup=False
) as dag:
    

    main_task()
