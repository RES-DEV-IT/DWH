from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG
from gs import insert_to_gs
from at_api import get_table

URL = "https://docs.google.com/spreadsheets/d/1mQzyzRzGDkKDPOQgYmkVKzIthCDhxEyXgHvC8YLr8Pw"

def pr_type_at(kom_projects):
    rows = kom_projects.all(
        fields=["Тип проекта"],
        formula="{Статус} != 'ЗАВЕРШЕН'",
        cell_format="string",
        time_zone="Europe/Moscow",  # Укажите ваш часовой пояс
        user_locale="ru"  # Укажите вашу локаль
    )

    data_pr_type = {}
    for row in rows:
        pr_type = row["fields"].get("Тип проекта")
        if pr_type is not None:
            pr_type = pr_type.replace(" ", "")
            if pr_type == "С/И":
                pr_type = "И/С"
            elif pr_type == "C":
                pr_type = "С"
            
            data_pr_type[pr_type] = data_pr_type[pr_type] + 1 if pr_type in data_pr_type else 1

    data_pr_type = [
        ("Инжиниринг", "И", data_pr_type.get("И", 0)),
        ("Сопровождение", "С", data_pr_type.get("С", 0)),
        ("Инжиниринг и сопровождение", "И/С", data_pr_type.get("И/С", 0)),
        ("Производство", "П", data_pr_type.get("П", 0)),
        ("Инжиниринг и производство", "И/П", data_pr_type.get("И/П", 0)),
        ("Итого:", "", data_pr_type.get("И", 0) + data_pr_type.get("С", 0) + data_pr_type.get("И/С", 0) + data_pr_type.get("П", 0) + data_pr_type.get("И/П", 0))
    ]
    return data_pr_type

def npp_at(kom_projects):
    rows = kom_projects.all(
        fields=["Объект поставки"],
        formula="{Статус} != 'ЗАВЕРШЕН'",
        cell_format="string",
        time_zone="Europe/Moscow",  # Укажите ваш часовой пояс
        user_locale="ru"  # Укажите вашу локаль
    )

    data_npp = {}
    for row in rows:
        npp = row["fields"].get("Объект поставки")

        if npp is not None:
            npp = npp.strip()
            if npp == "Rooppur":
                npp = "АЭС «Руппур»"

            data_npp[npp] = data_npp[npp] + 1 if npp in data_npp else 1

    data_npp = sorted(data_npp.items(), key=lambda item: item[1], reverse=True)

    return data_npp
    
def manuf_at(kom_projects):
    rows = kom_projects.all(
        fields=["Изготовитель по договору"],
        formula="{Статус} != 'ЗАВЕРШЕН'",
        cell_format="string",
        time_zone="Europe/Moscow",  # Укажите ваш часовой пояс
        user_locale="ru"  # Укажите вашу локаль
    )

    data_manuf = {}
    for row in rows:
        manufs = row["fields"].get("Изготовитель по договору")

        if manufs is not None:
            for manuf in manufs.split(", "):
                if manuf not in ('Shaanxi Zhonghuan Machinery Co.', 'DelVal', 'LC', 'RKC', 'EHO', 'Dembla', 'Nirmal', 'Hawa Tubes')\
                    and 'henan' not in manuf.lower()\
                    and 'ltd' not in manuf.lower()\
                    and 'starvalve' not in manuf.lower():
                    data_manuf[manuf] = data_manuf[manuf] + 1 if manuf in data_manuf else 1

    data_manuf = dict(sorted(data_manuf.items(), key=lambda item: item[1], reverse=True))
    return list(data_manuf.items())

@task
def main_task():
    kom_projects = get_table("Portal", "Portal 2.0.4", "KOM Projects")

    # === ТИП ПРОЕКТА ===
    data_pr_type = pr_type_at(kom_projects=kom_projects)
    print("\n---> PROJECT TYPE")
    print(data_pr_type, "\n")

    # === ОБЪЕКТ ПОСТАВКИ ===
    data_npp = npp_at(kom_projects=kom_projects)
    print("\n---> NPP")
    print(data_npp, "\n")
    
    # === ЗАВОДЫ ===
    data_manuf = manuf_at(kom_projects=kom_projects)
    hook = PostgresHook(postgres_conn_id="resdb_connection")
    records = hook.get_records(
        """
            select _manuf, count(*) as c
            from (
            select _manuf, _sheet_name, count(status) filter(where status = 'Finished') as finished_count, count(*) as total_count
            from (
                select
                _created_at,
                _manuf,
                _sheet_name, 
                jsonb_array_elements(content) ->> 'status' as status,
                max(_created_at) over(partition by _manuf, _sheet_name) max_created_at
                from stage.schedules_values
                --where _created_at < '2025-12-6'
            ) as t1
            where _created_at = t1.max_created_at
            group by _manuf, _sheet_name
            ) as t2
            where finished_count != total_count
            group by _manuf  
            order by c desc
        """
    )

    data_manuf += records
    data_manuf = sorted(data_manuf, key=lambda item: item[1], reverse=True)

    print("\n---> MANUF")
    print(data_manuf, "\n")

    # Вставка в гугл таблицу
    insert_to_gs(URL, data_pr_type, "По типу", append=False)
    insert_to_gs(URL, data_npp, "_По объектам поставки", append=False)
    insert_to_gs(URL, data_manuf, "_По ЗИ", append=False)
    

with DAG(
    dag_id="com_dashboards",
    default_args={"owner": "Artem", "retries": 0},
    start_date=datetime(2025, 11, 27, 4, 0, 0, 0),
    schedule_interval="0 20 * * mon-fri",
    catchup=False
):
    main_task()