import yaml
import re
from schedules.parse.utils import parse
from typing import List
from xml.etree import ElementTree as ET


PATH_TO_CONFIG = "/".join(__file__.replace("\\", "/").split("/")[:-2]) + "/CONFIG.yaml"

class ScheduleParser:
    def __init__(self, workbook):
        # Загружаем конфиг
        with open(PATH_TO_CONFIG) as stream:
            self.__config = yaml.safe_load(stream)
        
        # Ищем themes для workbook (для определения цвета ячейки)
        self.theme_colors = self.find_theme_colors(workbook=workbook)

        # Словарь для колонок, значений, цветов для каждого листа
        self.project_sheets = {}

        for sheet_name in workbook.sheetnames:
            sheet = workbook[sheet_name]

            if self.is_project_sheet(sheet_name=sheet_name) and sheet.sheet_state == "visible":
                # Парсим лист и получаем колонки, значения, цвета
                columns, values, background = parse(
                    sheet=workbook[sheet_name],
                    theme_colors=self.theme_colors,
                    colors=self.__config["COLORS"],
                    existing_components=self.__config["EXISTING_COMPONENTS"]
                )

                # Сохраняем колонки, значения, цвета
                self.project_sheets[sheet_name] = {
                    "columns": columns,
                    "values": values,
                    "background": background
                }

    def find_theme_colors(self, workbook):
        # Парсим XML темы
        theme_xml = workbook.loaded_theme
        root = ET.fromstring(theme_xml)

        # Находим схему цветов
        ns = {'a': 'http://schemas.openxmlformats.org/drawingml/2006/main'}
        clr_scheme = root.find('.//a:clrScheme', ns)

        # Получаем все цветовые элементы
        theme_colors = {
            'lt1': clr_scheme.find('./a:lt1/a:srgbClr', ns).attrib['val'],
            'dk1': clr_scheme.find('./a:dk1/a:srgbClr', ns).attrib['val'],
            'lt2': clr_scheme.find('./a:lt2/a:srgbClr', ns).attrib['val'],
            'dk2': clr_scheme.find('./a:dk2/a:srgbClr', ns).attrib['val'],
            'accent1': clr_scheme.find('./a:accent1/a:srgbClr', ns).attrib['val'],
            'accent2': clr_scheme.find('./a:accent2/a:srgbClr', ns).attrib['val'],
            'accent3': clr_scheme.find('./a:accent3/a:srgbClr', ns).attrib['val'],
            'accent4': clr_scheme.find('./a:accent4/a:srgbClr', ns).attrib['val'],
            'accent5': clr_scheme.find('./a:accent5/a:srgbClr', ns).attrib['val'],
            'accent6': clr_scheme.find('./a:accent6/a:srgbClr', ns).attrib['val'],
            'hlink': clr_scheme.find('./a:hlink/a:srgbClr', ns).attrib['val'],
            'folHlink': clr_scheme.find('./a:folHlink/a:srgbClr', ns).attrib['val']
        }

        return theme_colors


    def is_project_sheet(self, sheet_name: str) -> bool:
        return re.match(r'^P\d+.*', sheet_name)


    def get_typed_values_by_sheet_name(self, sheet_name: str, columns_and_types: List[tuple[str, str, str|None]], db_columns):
        print("get typed values", sheet_name)
        assert sheet_name in self.project_sheets, f"{sheet_name} not in self.project_sheets"

        return self.project_sheets[sheet_name].get_typed_dfs(columns_and_types=columns_and_types, db_columns=db_columns)
