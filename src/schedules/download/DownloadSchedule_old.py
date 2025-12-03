from gspread import Client, Spreadsheet, Worksheet, service_account
import gspread.utils as gs_utils
import yaml
import os


SERVICE_ACCOUNT_CREDS_PATH = "/".join(__file__.replace("\\", "/").split("/")[:-1]) + "/submitted-tables-download-v02-750e825a7950.json"
PATH_TO_CONFIG = "/".join(__file__.replace("\\", "/").split("/")[:-2]) + "/CONFIG.yaml"

class DownloadSchedule:
    def __init__(self, path_to_save: str) -> None:
        assert type(path_to_save) is str, "Path should be in string format"
        self.__client = service_account(filename=SERVICE_ACCOUNT_CREDS_PATH)
        self.__path_to_save = path_to_save
        
        if not os.path.isdir(self.__path_to_save):
            os.mkdir(self.__path_to_save)
            
        with open(PATH_TO_CONFIG) as stream:
            self.__config = yaml.safe_load(stream)

    def download_by_manuf_name(self, manuf_name: str):
        assert type(manuf_name) is str, "Manufacturer name should be in string format"
        assert manuf_name in self.__config["URLS"], f"Manufacturer name not in available names ({self.__config['URLS'].keys})"
        
        try:
            url = self.__config["URLS"][manuf_name]
            sheet = self.__client.open_by_url(url)
        
            excel_file_in_bytes = sheet.export(gs_utils.ExportFormat.EXCEL)

            with open(f"{self.__path_to_save}/{manuf_name}.xlsx", "wb") as binary_file:
                binary_file.write(excel_file_in_bytes)
            print(f"{self.__path_to_save}/{manuf_name}.xlsx - SAVED")

            return f"{self.__path_to_save}/{manuf_name}.xlsx"
        
        except Exception as e:
            print("Error", e)
            return None
        
if __name__ == "__main__":
    ds = DownloadSchedule("tmp")    
    ds.download_by_manuf_name(manuf_name="DelVal")
