from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import numpy as np
import re
import yaml
import time


PATH_TO_CONFIG = "/".join(__file__.replace("\\", "/").split("/")[:-2]) + "/CONFIG.yaml"
SERVICE_ACCOUNT_CREDS_PATH = "/".join(__file__.replace("\\", "/").split("/")[:-1]) + "/excellent-ship-487111-n9-bdb8d109f4e5.json"


class DownloadSchedule:
    def __init__(self):
        credentials = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_CREDS_PATH,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        self.service = build('sheets', 'v4', credentials=credentials)

        with open(PATH_TO_CONFIG) as stream:
            self.__config = yaml.safe_load(stream)

    def is_project_sheet(self, sheet_name: str, manuf_name: str) -> bool:
        """Является ли имя листа проектом"""
        if manuf_name == "Jsons":
            return "orders" in sheet_name
        else:
            return re.match(r'^P\d+.*', sheet_name)

    def download_by_manuf_name(self, manuf_name: str):
        assert type(manuf_name) is str, "Manufacturer name should be in string format"
        assert manuf_name in self.__config["URLS"], f"Manufacturer name not in available names ({self.__config['URLS'].keys})"
        
        url = self.__config["URLS"][manuf_name]
        spreadsheet_id = url.split("/")[-1]

        spreadsheet_info = self.service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()

        sheets = spreadsheet_info['sheets']
        visible_sheets = [sheet['properties']['title'] for sheet in sheets if sheet['properties'].get('hidden') != True]

        print("SHEETS", visible_sheets)

        result = {}

        for sheet_name in visible_sheets:
            if self.is_project_sheet(sheet_name=sheet_name, manuf_name=manuf_name):
                print(sheet_name)

                # Получаем значения на листе
                values_result = self.service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{sheet_name}'"
                ).execute()

                max_row_len_values = max([len(row) for row in values_result["values"]])
                for row in values_result["values"]:
                    row.extend(['']*(max_row_len_values - len(row)))
                assert len(values_result["values"]) > 0, f"Empty sheet - SHEET NAME{sheet_name}"
                num_rows = len(values_result["values"])
                num_columns = len(values_result["values"][0])
                num_columns_letter = self.column_number_to_letter(num_columns)
                
                format_result = self.service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id,
                    ranges=[f"'{sheet_name}'!A1:{num_columns_letter}{num_rows}"],
                    fields='sheets(data(rowData(values(userEnteredFormat,formattedValue))))'
                ).execute()

                print("SLEEP 1 sec.")
                time.sleep(1) # Вызываем небольшую задержку, чтобы не ловить rate limit на скачку листов
        
                pono_row_idx = None
                for i, row in enumerate(values_result["values"]):
                    if "RES PO" in row and manuf_name == "Jsons":
                        pono_row_idx = i
                    elif 'PO No.' in row:
                        pono_row_idx = i

                assert pono_row_idx is not None, "Cant find PO No. in rows"

                values_row_idx = pono_row_idx + 1
                while values_result["values"][values_row_idx][0] == '':
                    values_row_idx += 1

                # Получаем имена колонок (и индексы колонок, которые нужно выбросить)
                ignore_col_idxs = []
                columns = []
                if values_row_idx == pono_row_idx + 2: # Перед нами график с делением на компоненты
                    current_component = None
                    for col_idx, col_name in enumerate(values_result["values"][pono_row_idx]):
                        if col_name in ["BODY", "DISC", "WNF", "END CONNECTOR", "BONNET", "CAGE", "FLANGE", "WEDGE", "SEAT", "COVER"]:
                            current_component = col_name
                            columns.append(current_component + ' ' + values_result["values"][pono_row_idx + 1][col_idx])
                        elif col_name != '':
                            columns.append(col_name)
                        else:
                            if values_result["values"][pono_row_idx + 1][col_idx] != '':
                                columns.append(current_component + ' ' + values_result["values"][pono_row_idx + 1][col_idx])
                            else:
                                ignore_col_idxs.append(col_idx)
                elif values_row_idx == pono_row_idx + 1: # Перед нами график без компонент (LC, RKC)
                    for col_idx, col_name in enumerate(values_result["values"][pono_row_idx]):
                        if col_name != '':
                            columns.append(col_name)
                        else:
                            ignore_col_idxs.append(col_idx)
                else:
                    assert 1==2, f"Странное строение графика, проверь шапку в листе - {sheet_name}"
                    

                values = [['NULL'] * (max_row_len_values) for _ in range(len(values_result["values"]) - values_row_idx)]
                bgs = [['NULL'] * (max_row_len_values) for _ in range(len(values_result["values"]) - values_row_idx)]

                sheet_data = format_result["sheets"][0]["data"][0]
                
                if 'rowData' in sheet_data:
                    for row_index, row in enumerate(sheet_data['rowData']):
                        if row and 'values' in row and row_index >= values_row_idx:
                            for col_index, cell in enumerate(row['values']):
                                if cell and col_index not in ignore_col_idxs:
                                    # Получаем значение ячейки
                                    cell_value = cell.get('formattedValue', '')
                                    
                                    # Получаем цвет заливки
                                    rgb_color = (255, 255, 255) # стоковый цвет
                                    if 'userEnteredFormat' in cell:
                                        format_info = cell['userEnteredFormat']
                                        if 'backgroundColor' in format_info:
                                            bg_color = format_info['backgroundColor']
                                            red = int(bg_color.get('red', 0) * 255)
                                            green = int(bg_color.get('green', 0) * 255)
                                            blue = int(bg_color.get('blue', 0) * 255)
                                            rgb_color = (red, green, blue)
                                    
                                    values[row_index - values_row_idx][col_index] = cell_value.replace("'", '')
                                    bgs[row_index - values_row_idx][col_index] = str(rgb_color)
                                   
                values = np.array(values)
                values = np.delete(values, ignore_col_idxs, axis=1)
                bgs = np.array(bgs)
                bgs = np.delete(bgs, ignore_col_idxs, axis=1)
                pg_columns_names = []
                for column_name in columns:
                    column_name = column_name.lower().replace("/", "").replace(".", "")
                    column_name = '_'.join(column_name.split())
                    pg_columns_names.append(column_name)
                result[sheet_name] = {
                    "values": values,
                    "background": bgs,
                    "columns": pg_columns_names
                }
        return result
                # KKS
                # kks_index = columns.index("KKS")
                # values[:, kks_index] = kks_transformer(values[:, kks_index].tolist())
                
                # columns_with_custom = ["_created_at", "_project_row_number"]
                # columns_with_custom.extend(columns)
                # columns_with_custom.extend([col + "_bg" for col in columns])


                # insert_sql = 'INSERT INTO stg_delval values\n'
                # created_at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                # for i, row in enumerate(values):
                    
                #     row_with_custom_values = [created_at, str(i+1)]
                #     row_with_custom_values.extend(row)
                #     row_with_custom_values.extend(bgs[i])
                #     row_with_custom_values = [f"'{v}'" for v in row_with_custom_values]
                #     insert_sql += f"({', '.join(row_with_custom_values)}),\n"

                # sql = f"CREATE TABLE IF NOT EXISTS stg_delval({',\n'.join([f'"{col_name}"' + ' TEXT' for col_name in columns_with_custom])})"
                
                
                # if not created:
                #     created = True
                #     hook.run(sql, handler=silent_handler)
                # hook.run(insert_sql[:-2], handler=silent_handler)

        

    def column_number_to_letter(self, column_number):
        """
        Конвертирует номер колонки в буквенное обозначение (1->A, 2->B, ..., 27->AA, etc.)
        """
        letter = ''
        while column_number > 0:
            column_number, remainder = divmod(column_number - 1, 26)
            letter = chr(65 + remainder) + letter
        return letter

if __name__ == "__main__":
    ds = DownloadSchedule()
    result = ds.download_by_manuf_name(manuf_name="Jsons")

    print(result)
