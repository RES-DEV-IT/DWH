from gspread import service_account


SERVICE_ACCOUNT_CREDS_PATH = "./plugins/schedules/download/submitted-tables-download-v02-750e825a7950.json"


def insert_to_gs(url, data_to_insert, sheet_name, append=True):
    if len(data_to_insert) == 0:
        return 
    
    client = service_account(SERVICE_ACCOUNT_CREDS_PATH)

    worksheet = client.open_by_url(url).worksheet(sheet_name)

    if append:
        worksheet.append_rows(
            data_to_insert,
            value_input_option="USER_ENTERED"
        )
    else:
        row_start = 2
        col_start = 1

        row_len = len(data_to_insert[0])
        data_to_insert = data_to_insert + [[''] * row_len for i in range(10000)]
        worksheet.update(
            f"R{row_start}C{col_start}:R{row_start + len(data_to_insert) - 1}C{col_start + len(data_to_insert[0]) - 1}",
            data_to_insert,
            raw=False
        )