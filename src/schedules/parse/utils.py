import numpy as np
from typing import Tuple


def parse(sheet, theme_colors, colors, existing_components):
    print("ProjectSheet parsing", sheet.title)

    # Getting range
    last_row, last_column = get_last_row_and_last_column(sheet)

    # Getting values
    raw_values = [row for row in sheet.iter_rows(min_row=1, max_row=last_row, min_col=1, max_col=last_column, values_only=True)]
    raw_values = np.array(raw_values)

    # Getting raw background colors
    background_colors = []
    for row in sheet.iter_rows(min_row=1, max_row=last_row, min_col=1, max_col=last_column):
        background_row = []
        for cell in row:
            rgb_color = get_cell_bg(cell=cell, theme_colors=theme_colors)
            if rgb_color not in colors:
                background_row.append("NULL")
            else:
                background_row.append(colors[rgb_color])
        background_colors.append(background_row)

    raw_background_colors = np.array(background_colors)

    # Extract usefull information
    first_column = raw_values[:, 0]
    try:
        pono_idx = np.where(first_column == "PO No.")[0][0] # MANUFACTURES
    except:
        pono_idx = np.where(first_column == "RES PO")[0][0] # JSONS
    
    columns_names = []
    columns_idxs = []
    head_size = None
    if first_column[pono_idx + 1] is None: # 3 rows head
        for i, col_name in enumerate(raw_values[pono_idx]):
            if col_name is not None:
                if col_name in existing_components:
                    i1 = i
                    columns_names.append(col_name + " " + raw_values[pono_idx + 1][i1])
                    columns_idxs.append(i1)
                    i1 += 1
                    while raw_values[pono_idx][i1] is None and raw_values[pono_idx + 1][i1] is not None:
                        columns_names.append(col_name + " " + raw_values[pono_idx + 1][i1])
                        columns_idxs.append(i1)
                        i1 += 1
                else:
                    columns_names.append(col_name)
                    columns_idxs.append(i)
        head_size = 2
    else: # 2 rows
        for i, col_name in enumerate(raw_values[pono_idx]):
            columns_names.append(col_name)
            columns_idxs.append(i)
        head_size = 1

    values = raw_values[pono_idx+head_size:, columns_idxs]
    background_colors = raw_background_colors[pono_idx+head_size:, columns_idxs]

    pg_columns_names = []
    for column_name in columns_names:
        column_name = column_name.lower().replace("/", "").replace(".", "")
        column_name = '_'.join(column_name.split())
        pg_columns_names.append(column_name)
    
    columns = pg_columns_names
    values = values
    background = background_colors
    
    assert len(columns) == len(values[0])
    assert len(values[0]) == len(background[0])
    assert len(values) == len(background)
    return columns, values, background

def get_last_row_and_last_column(sheet) -> Tuple[int, int]:
        last_row = 0
        last_column = 0
        for row in sheet.iter_rows():
            for cell in row:
                if cell.value is not None:
                    last_row = max(last_row, cell.row)
                    last_column = max(last_column, cell.column)

        return last_row, last_column

def get_cell_bg(cell, theme_colors) -> Tuple[int, int, int]:
    if cell.value == "NA": # Если в ячейке NA, то возвращаем серый цвет (#999999)
        return '(153, 153, 153)'
    if cell.fill is None:
        return '(255, 255, 255)'
    if cell.fill.fill_type is None:
        return '(255, 255, 255)'
    if cell.value is None and cell.value == "":
        return '(255, 255, 0)'
    
    color = cell.fill.start_color
    if color.type == 'rgb':
        hex = color.rgb[2:]
        return str(tuple(int(hex[i:i+2], 16) for i in (0, 2, 4)))
    elif color.type == 'theme':
        color_index = color.theme
        if color_index < len(theme_colors.keys()):
            hex = list(theme_colors.values())[color_index]
            return str(tuple(int(hex[i:i+2], 16) for i in (0, 2, 4)))
        else:
            return None
    return None
