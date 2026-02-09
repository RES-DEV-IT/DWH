from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Variable
from autosave.AutoSaver import AutoSaver
from autosave.save import save
import subprocess
import os
from pyairtable import Table, Base, Api
import shutil
from autosave.AT.airtableApi import get_table


airtable_pit = get_table("Portal", "Portal 2.0.4", "PIT")#api.table(BASE_ID, f"PIT")


SHARE_PROJECTS = [
 '0 - Инструменты',
 '1 - Внутренние проекты',
 '2 - Закрытые проекты',
 '3 - Рабочие группы',
 'P05 - ПАБ - Руппур',
 'P07 (19-40) - ПАБ - КуданКулам',
 'P08 (19-20) - ЗД ВД - КурАЭС-2',
 'P10 (19-74) - ГН - КурАЭС-2',
 'P12 (20-41) - АТН - Аккую АЭС',
 'P15 (19-12-Р) - РА - РупАЭС',
 'Thumbs.db',
 'Р100 (23-49) - ЗД, ЗТД и КР - КурАЭС-2',
 'Р101 (21-74) - РК - КурАЭС-2',
 'Р102 (23-41) - НА - Эль-Дабаа',
 'Р103 (23-68) - КШ - Эль-Дабаа',
 'Р104 (23-27) - ЗТД - Эль-Дабаа',
 'Р105 (23-74) - РК - КурАЭС-2',
 'Р106 (23-77) - ТПА - Аккую',
 'Р107 (22-67) - ОК - Эль-Дабаа',
 'Р108 (23-75) - НА - Эль-Дабаа',
 'Р109 (24-06) - КЗ - Аккую',
 'Р110 (24-07) - КЗ и ЗТД - Аккую',
 'Р111 (23-76) - КР, ЗТД - Руппур',
 'Р112 (23-43) - НА - Аккую',
 'Р113 (24-04) - НА - Руппур',
 'Р114 (24-13) - КР, ЗТД - Руппур',
 'Р115 (22-37) - НА - Эль-Дабаа',
 'Р116 (23-52) - НА - КурАЭС-2',
 'Р117 (19-75) - НА - КуданКулам',
 'Р118 - ЗД - КуданКулам',
 'Р119 (23-31) - ЗД - Эль-Дабаа',
 'Р120 (23-80) - НА - Эль-Дабаа',
 'Р121 (24-18) - КШ и КР - Руппур',
 'Р122 (24-17) - КР - КурАЭС-2',
 'Р123 (23-29) - НА - Эль-Дабаа',
 'Р124 (24-43) - КЗ, ЗД, КШ - Аккую',
 'Р125 (24-44) - КЗ, ЗД, КР - Аккую',
 'Р126 (24-45) - КЗ и КР - Аккую',
 'Р127 (24-28) - КП - Аккую',
 'Р128 (24-22) - ВК и ЗТД - Аккую',
 'Р129 (23-85) - НА - Эль-Дабаа',
 'Р130 (24-29) - ГМО - Эль-Дабаа',
 'Р131 (23-51) - НА - Эль-Дабаа',
 'Р132 (24-75) - КХОП - Аккую',
 'Р133 (24-67) - КХОП - Аккую',
 'Р134 (24-77) - ЭП - Аккую',
 'Р135 (24-14) - НА - Эль-Дабаа',
 'Р136 - ГПЗ ДУ800 - Руппур',
 'Р137 (24-78) - КЗ и КР - Аккую',
 'Р138 (24-79) - КЗ и КР - Аккую',
 'Р139 (24-21) - НА - О53',
 'Р140 (24-20) - ОК - Эль-Дабаа',
 'Р141 (24-72) - ГПЗ - Руппур',
 'Р142 (20-19) - НА - Аккую',
 'Р143 (24-91) - ЗРА - Аккую',
 'Р144 (24-86) - КШ - Аккую',
 'Р145 (24-60) - РК - Руппур',
 'Р146 (24-92) - КП - Аккую',
 'Р147 (25-03) - ТПА - Аккую',
 'Р148 (25-04) - ТПА - Аккую',
 'Р149 (24-21) - НА - О53',
 'Р150 (23-69) - РА - Эль-Дабаа',
 'Р151 (24-63) - КШ - Аккую',
 'Р152 (24-104) - ПК - Аккую',
 'Р153 (24-104) - ПК - Аккую',
 'Р154 (24-104) - КЗ - Аккую',
 'Р155 (24-104) - КЗ - Аккую',
 'Р156 (24-104) - КЗ - Аккую',
 'Р157 (24-104) - КЗ - Аккую',
 'Р158 (24-108) - КЗ- Аккую',
 'Р159 (24-16) - ЗД - Эль-Дабаа',
 'Р16 (20-64) - ЗД -  КурАЭС-2',
 'Р160  (25-06) - МК - Аккую',
 'Р161 (24-12) - Эль-Дабаа',
 'Р162 (24-15) - КШ - Эль-Дабаа',
 'Р167 (25-12) - РК - Аккую',
 'Р169 (25-11) - РК - Аккую',
 'Р17 (20-102) - ЗА НД - РупАЭС',
 'Р19 (21-49) - ТО - Тяньвань-Сюйдапу АЭС',
 'Р20 (19-02) - РА - КурАЭС-2',
 'Р26 (20-109) – ЗД ВД – КурАЭС-2',
 'Р32 (21-09) – ОА – Руппур',
 'Р35 (21-13) – ЗТД – Аккую',
 'Р37 (21-42) - ПК - Аккую',
 'Р38 (21-64) - КЗ+КШ, ЗТД, ЗД - Аккую',
 'Р39 (21-41) - ЗТД РА - Аккую',
 'Р40 (21-62) - РА - Аккую',
 'Р41 (24-03) - КШ, ЗТД, ОК, РК - КуданКулам',
 'Р42 (21-40) - ПК - Руппур',
 'Р43 (21-46) - ТО - Аккую',
 'Р44 (21-48) - РК - Аккую (21-069m)',
 'Р45 (21-61) - РК - Аккую',
 'Р47 (21-55) - РК - КуАЭС-2',
 'Р48 (21-45) - РК - Аккую',
 'Р49 (21-65) - РК - Аккую',
 'Р50 (21-57) - РК LCQ - ТАЭС, САЭС',
 'Р51 (21-66) - РА - Аккую',
 'Р53 (20-60) - ГК - Руппур',
 'Р54 (22-11) - КП - Аккую',
 'Р55 (22-34) - КЗ - Аккую',
 'Р56 (22-06) - РК - ТАЭС, САЭС',
 'Р57 (22-39) - ЗД - Аккую',
 'Р58 (22-09) - Отл - КуданКулам',
 'Р59 (22-29) - РК - Аккую',
 'Р60 (22-38) - ВК - Аккую',
 'Р61 (22-43) - КШ и ОК - Аккую',
 'Р64 (22-24) - ЗТД - Аккую',
 'Р65 (22-08) - КР, КО - Аккую',
 'Р67 (22-41) - РК - КурАЭС-2',
 'Р68 (22-53) - РА - Руппур',
 'Р69 (22-27) - РК и РД - Аккую',
 'Р70 (22-50) - КП - Аккую',
 'Р71 (22-71) - ЗТД - Аккую',
 'Р72 (22-12) - НА - Эль-Дабаа',
 'Р73 (22-59) - КР и КО - Аккую',
 'Р74 (22-80) - КЗ и РК - Аккую',
 'Р75 (22-83) - ПК, РК, РЗ - Аккую',
 'Р76 (22-77) - КП - Аккую',
 'Р77 (22-57) - НА - Аккую',
 'Р78 (22-69) - КШ - Эль-Дабаа',
 'Р79 (22-47) - ЗТД - Эль-Дабаа',
 'Р80 (23-03) - КО - КурАЭС-2',
 'Р81 (23-15) - РК - КуАЭС-2',
 'Р82 (23-16) - ОК - КуАЭС-2',
 'Р83 (23-17) - ЗТД - КуАЭС-2',
 'Р84 (23-01) - ГПЗ - Аккую',
 'Р85 (19-61) - НА - Аккую',
 'Р86 (23-07) - ЗД - Аккую',
 'Р87 (22-79) - ЗТД - Эль-Дабаа',
 'Р88 (23-10) - НА - КурАЭС-2',
 'Р89 (22-81) - КР - Аккую',
 'Р90 (23-28) - КР - Аккую',
 'Р91 (23-30) - КЗ - Аккую',
 'Р92 (23-22) - КР - Аккую',
 'Р93 (23-19) - ЗД - Аккую',
 'Р94 (23-55) - КШ и ЗД - Аккую',
 'Р95 (23-54) - КШ и ЗД - Аккую',
 'Р96 (23-36) - КЗ - Аккую',
 'Р97 (23-60) - ВК - Аккую',
 'Р98 (23-34) - ЗД - Аккую',
 'Р99 (23-72) - ЗД, КШ и КО - Аккую'
]


@task
def auto_save():
    if os.path.isdir("/AUTOSAVE_SHARE_COPY"):
        shutil.rmtree("/AUTOSAVE_SHARE_COPY")

    # Создаем объект для выгрузки инфы с портала
    saver = AutoSaver("/AUTOSAVE_SHARE_COPY", SHARE_PROJECTS)
    
    # Выгружаем "Uploaded" строки с портала
    saver.fetch_records()
    print("\n\n---> NUMBER OF RECORDS FETCHED", len(saver.records), "\n\n")
    
    # Сохраняем файлы локально (на DEV сервер)
    info = []

    for record in saver.records:
        
        path_on_share, all_filenames = save(record=record, delete_after=True)

        info.append({
            "record_id": record.id,  # ID строки в AirTable
            "path_on_share": path_on_share,
            "filenames": all_filenames
        })
        
    return {"info": info}

@task
def copy_to_share():
    print(os.listdir("/AUTOSAVE_SHARE_COPY"))
    var = Variable.get("AUTOSAVE_DEPLOY")
    print("AUTOSAVE_DEPLOY status:", var)
    if var == "TRUE":
        try:
            subprocess.run("rsync -avh --progress /AUTOSAVE_SHARE_COPY/ './syncer/'", shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Ошибка! Код: {e.returncode}")
            print("Ошибка:", e.stderr)
        except Exception as e:
            print("Неизвестная ошибка:", str(e))

# @task
# def update_airtable(saved_records_ids, paths):
#     print(saved_records_ids, paths)
    
#     for path in paths:
#         docker_path = path.replace("Z:/Проекты/", "./syncer/")
#         if not os.path.isfile(docker_path):
#             raise ValueError(f"NON SYNCED file: {path}")
        
#     var = Variable.get("AUTOSAVE_DEPLOY")
#     print("AURTOSAVE_DEPLOY status:", var)
#     if var == "TRUE":
#         airtable_records = []

#         for at_id, path in zip(saved_records_ids, paths):
#             airtable_records.append({
#                 "id": at_id, 
#                 "fields": {
#                     "Status": "ICP verification",
#                     "autosaved": True,
#                     "[T] Окончание сохранения (чек)": True,
#                     "Enter server path": path,
#                 }
#             })
#         airtable_pit.batch_update(airtable_records)

@task
def update_airtable(info):
    var = Variable.get("AUTOSAVE_DEPLOY")
    print("AURTOSAVE_DEPLOY status:", var)
   
    airtable_records = []
    for record_info in info["info"]:
        
        print("INFO", record_info)

        success = 0
        for file in record_info["filenames"]:
            
            file_path = file["path"].replace("/AUTOSAVE_SHARE_COPY", "./syncer")
            file_type = file["type"]

            print("├──", file_path)
            print(f"      ├── type: {file_type}")
            if file_type == "file":
                if os.path.isfile(file_path):
                    print(f"      └── exists: True")
                    success += 1
                else:
                    print(f"      └── exists: False")
            elif file_type == "dir":
                if os.path.isdir(file_path):
                    print(f"      └── exists: True")
                    success += 1
                else:
                    print(f"      └── exists: False")
            else:
                raise ValueError(f"WRONG FILE TYPE: {file_type}")
        
        if success == len(record_info["filenames"]):
            airtable_records.append({
                "id": record_info["record_id"],
                "fields": {
                    "Status": "ICP verification",
                    "autosaved": True,
                    "[T] Окончание сохранения (чек)": True,
                    "Enter server path": record_info["path_on_share"],
                }
            })
    
    if var == "TRUE":
        airtable_pit.batch_update(airtable_records)

with DAG(
    dag_id=f'autosave_dag',
    default_args={
        "owner": "Artem",
        "retries": 0,
        "retry_delay": timedelta(minutes=5)
    },
    start_date=datetime(2025, 6, 17, 0, 0, 0),
    schedule_interval="30 6 * * mon-fri",
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=['automatization']
) as dag:
    auto_save_output = auto_save()
    copy_task = copy_to_share()
    update_task = update_airtable(
        info = auto_save_output
        # saved_records_ids=auto_save_output["saved_records_ids"],
        # paths=auto_save_output["paths"]
    )

    auto_save_output >> copy_task >> update_task
