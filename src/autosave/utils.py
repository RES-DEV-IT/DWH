import os
from typing import List
import re


# Функция для поиска папки проекта на шаре
def find_project_folder(pono: str, share_projects: List[str]) -> str:
    
    # P132/P133 -> P132
    # print("OLD PONO", pono)
    # pono = pono.split("/")[0]
    # print("NEW PONO", pono)

    for prj_name in share_projects:
        # print(prj_name.split(" ")[0].replace("Р", "P"), pono)
        if prj_name.split(" ")[0].replace("Р", "P") == pono:
            return prj_name
        
    raise ValueError(f"Project name share is None. Can't find project by given PO: {pono}")
    
# Соединение item-ов в серии
def items_agg(items: List[str]) -> List[str]:
    # "Дистанция" между item-ами 
    def calc_distance(i1, i2):
        if "." in i1 and "." in i2:
            i1_spl = [int(i1_part) for i1_part in i1.split('.')]
            i2_spl = [int(i2_part) for i2_part in i2.split('.')]
            return abs(i1_spl[0] - i2_spl[0]) + abs(i1_spl[1] - i2_spl[1])
        
        elif "." not in i1 and "." not in i2: # Если работаем с SDTork
            return abs(int(i1) - int(i2))

        raise ValueError(f"Different types of items: item1 = {i1}, item2 = {i2}")

    def process_seria(seria):
        
        if len(seria) == 1:
            seria_string = f"{seria[0]}"
        elif len(seria) == 2:
            seria_string = f"{seria[0]},{seria[1]}"
        else:
            seria_string = f"{seria[0]}-{seria[-1]}"
        return seria_string
    
    # Если в ячейке написано ALL
    if len(items) == 1:
        if items[0] in ["ALL", "All"]:
            return ["ALL"]

    result = []
    
    items = sorted(items, key=lambda x: [int(s) for s in x.split(".")])
    
    seria = []
    for item in items:
        
        if len(seria) == 0:
            seria.append(item)
        else:
            distance = calc_distance(seria[-1], item)
            
            if distance == 1:
                seria.append(item)
            else:
                seria_string = process_seria(seria)
                result.append(seria_string)
                
                seria = [item]
                
    if len(seria) != 0:
        seria_string = process_seria(seria)
        result.append(seria_string)
    return result


def lots_agg(lots):

    # Если лот всего один
    if len(lots) == 1: return lots[0]

    # Иначе, проверяем, что лот записан правильно
    pattern = r'^LOT\d+$'
    numbers = []
    for lot in lots:
        if re.fullmatch(pattern, lot):
            numbers.append(int(lot[3:]))
        else:
            raise ValueError(f"LOT doesn't math pattern: {lot}")

    numbers = [str(n) for n in sorted(numbers)]

    return f"LOT {', '.join(numbers)}"
