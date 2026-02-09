from autosave.Record import Record
from typing import List
from autosave.AT.airtableApi import get_table


class AutoSaver:
    def __init__(self, root_folder: str, share_projects: List[str]) -> None:
        self.__token_name = "Portal"
        self.__base_name  = "Portal 2.0.4"

        self.pit_base = get_table(
            token_name=self.__token_name,
            base_name=self.__base_name,
            table_name="PIT")
        

        self.init_id2manuf()

        self.root_folder = root_folder
        self.share_projects = share_projects
        
    def init_id2manuf(self) -> None:
        # Загружаем информацию из таблицы Manufacturers (ID -> manuf_name)
        manuf_base = get_table(self.__token_name, self.__base_name, "Manufacturers")
        manufs = manuf_base.all()
        self.ID2MANUF = dict([(r["id"], r["fields"]["Name"]) for r in manufs])
    
    def fetch_records(self):

        # Получаем записи с AirTable, которые нужно сохранять на Share
        records = self.pit_base.all(
            fields=[
                "Status", "Files", "Manufacturer", "Project", "PO No. (RES)", "Stage", "LOT",
                "Item", "Task",
                "Short name in English (from Document Type)",
                "Short name in Russian (from Document Type)",
                '[T] Rev# (for formula "Task")', "All Items (from Project)",
                "Last uploaded by Manufacturer",
                "Rev.0 files", "Rev.1 files", "Rev.2 files", "Rev.3 files", "Rev.4 files", "Rev.5 files",
                "Rev.6 files", "Rev.7 files", "Rev.8 files", "Rev.9 files", "Rev.10 files",
                "Last Modified",
                "Bare",
                "Rev.0 dtd.", "Rev.1 dtd.", "Rev.2 dtd.", "Rev.3 dtd.", "Rev.4 dtd.", "Rev.5 dtd.",
                "Rev.6 dtd.", "Rev.7 dtd.", "Rev.8 dtd.", "Rev.9 dtd.", "Rev.10 dtd."
            ],
            formula="""
            AND(
                AND(
                    {Manufacturer}!='SDTork',
                    AND(
                        OR({Stage}='KPD1.1', {Stage}='KPD1.2', {Stage}='KPD3', {Stage}='KPD4', {Stage}='KPD5'),
                        {Status} = 'Uploaded'
                    )
                ),
                {autosaved}=BLANK()
            )
            """ 
        )

        # Пытаемся распарсить записи, оборачивем каждую запись в класс Record
        self.records = []
        
        for record in records:
            try:
                self.records.append(
                    Record(
                        record,
                        self.ID2MANUF,
                        self.root_folder,
                        self.share_projects
                    )
                )
            except Exception as e:
                print("ERROR", e)
        print("Records fetched")

if __name__ == "__main__":
    saver = AutoSaver("./test")
    saver.fetch_records()