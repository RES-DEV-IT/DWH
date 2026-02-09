from autosave.utils import find_project_folder, items_agg, lots_agg
from typing import List


class Record:
    def __init__(self, record, ID2MANUF, root_folder, share_projects: List[str]) -> None:
        self.root_folder = root_folder
        self.profile = {}

        # ID
        self.id = record["id"]

        # Bare - номер строки
        self.profile["bare"] = record["fields"]["Bare"]
        print("PARSING RECORD... bare_number =", self.profile["bare"])

        # Manufacturer
        assert len(record["fields"]["Manufacturer"]) == 1
        assert record["fields"]["Manufacturer"][0] in ID2MANUF
        self.profile["manuf_name"] = ID2MANUF[record["fields"]["Manufacturer"][0]]

        # PO No
        assert len(record["fields"]["PO No. (RES)"]) == 1
        self.profile["pono"] = record["fields"]["PO No. (RES)"][0].replace("/", "-").replace("\\", "_")
        
        # Project name (P125-126-DL-1 -> P125 -> Р125 (24-44) - КЗ, ЗД, КР - Аккую)
        self.profile["project_name_share"] = find_project_folder(
            self.profile["pono"].split("-")[0],
            share_projects=share_projects
        )
        
        # Stage
        self.profile["stage"] = record["fields"]["Stage"]

        # Document type
        assert len(record["fields"]["Short name in English (from Document Type)"]) == 1
        self.profile["doc_type_en"] = record["fields"]["Short name in English (from Document Type)"][0].replace("\\", "_").replace("/", "_")
        self.profile["doc_type_ru"] = record["fields"]["Short name in Russian (from Document Type)"][0].replace("\\", "_").replace("/", "_")

        # LOT 
        self.profile["lot"] = lots_agg(record["fields"]["LOT"])

        # Items
        assert len(record["fields"]["Item"]) > 0
        self.profile["items"] = record["fields"]["Item"]

        # Items Agg
        self.profile["item_agg"] = items_agg(self.profile["items"])

        # All items
        if record["fields"]["All Items (from Project)"][0] is not None:
            self.profile["all_items"] = record["fields"]["All Items (from Project)"][0].split(",")
        else:
            self.profile["all_items"] = None
        
        # Last modified
        last_modified = record["fields"].get("Last uploaded by Manufacturer")
        if last_modified is None:
            last_modified = record["fields"].get("Last Modified")
        
        self.profile["last_modified"] = last_modified[:10].replace("-", ".")

        # Rev
        self.profile["rev#"] = str(record["fields"]['[T] Rev# (for formula "Task")'])
        self.profile["rev_date"] = record["fields"][f"Rev.{str(self.profile['rev#'])} dtd."][:10].replace("-", ".")

        # Files
        self.profile["files"] = [{
            "url": file["url"],
            "filename": file["filename"].replace("/", "_"),
            "size": file["size"]
        } for file in record["fields"][f"Rev.{str(self.profile['rev#'])} files"]]
        
        # Техника или Кооперация
        self.profile["folder"] = self.folder()

        # Является ли строка про все айтемы сразу
        self.profile["is_items_all"] = self.is_items_all()

        print("RECORD PARSED bare_num =", self.profile["bare"])

    def folder(self) -> str:
        if (self.profile["stage"] == "KPD2"
            and self.profile["doc_type_ru"] not in ["Зоны отливок без радиографии", "Инвойс на предоплату", "Программы заводских испытаний"]
           ):
            return "2 - Техника"
        else:
            return "4 - Кооперация"

    def is_items_all(self):
        if self.profile["all_items"] is None: 
            return False
        
        if set(self.profile["all_items"]) == set(self.profile["items"]):
            return True
        return False
            
    def path(self) -> str:
        path = self.root_folder + "/"
        # Проект
        path += self.profile["project_name_share"] + "/"

        # "2 - Техника" или "4 - Кооперация"
        path += self.profile["folder"] + "/"

        # Доп папка, если "2 - Техника"
        if self.profile["folder"] == "2 - Техника": path += "Чертежи, модели, ОЛ/"

        if self.profile["folder"] == "4 - Кооперация":
            # Классификация на Насосы или ТПА в зависимости от ЗИ
            if self.profile["manuf_name"] == "LC":
                path += "Насосы/"
            else:
                path += "ТПА/"

        # Завод
        path += self.profile["manuf_name"] + "/"
        
        # Доп папка, если "4 - Кооперация"
        if self.profile["folder"] == "4 - Кооперация":
            path += "KPD/"
            
        # PO
        path += self.profile["pono"] + "/"

        if self.profile["folder"] == "4 - Кооперация":
            path += self.profile["stage"] + "/"
            path += self.profile["doc_type_en"] + "/"
            path += self.profile["lot"] + "/"
            if self.profile["is_items_all"]:
                path += "ALL/"
            else:
                path += "Item " + ", ".join(self.profile["item_agg"]) + "/"
            
            path += self.profile["rev_date"] + " - rev." + self.profile["rev#"]
        else:
            path += self.profile["doc_type_ru"] + "/"

            if self.profile["is_items_all"]:
                path += f"{self.profile['rev_date']} - Ревизия {self.profile['rev#']}"
            else:
                path += f"{self.profile['rev_date']} - Ревизия {self.profile['rev#']} ({", ".join(self.profile["item_agg"])})"
                
        return path
    
    def iterate_through_files(self):
        for file in self.profile["files"]:
            yield file["url"], file["filename"].replace("/", "_"), file["size"]
     