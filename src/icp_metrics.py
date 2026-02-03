import pandas as pd

def shifts(hook, manuf_name):
    records = hook.get_records(f"""
        SELECT 
            TO_CHAR(_created_at, 'DD.MM.YYYY'),
            _manuf,
            _sheet_name,
            replace(po_item, ',', '.'),
            key,
            TO_CHAR(try_parse_date(old_value), 'DD.MM.YYYY'),
            TO_CHAR(try_parse_date(current_value), 'DD.MM.YYYY')
        FROM schedules_changes
        WHERE try_parse_date(old_value) IS NOT NULL
            AND try_parse_date(current_value) IS NOT NULL
            AND try_parse_date(old_value) < try_parse_date(current_value)
            AND old_value != current_value
            AND _manuf = '{manuf_name}'
    """)
    return records


def kks_vs_qty(hook):
    KKS_VS_QTY_QUERY = """
-- === KKS vs QTY ===
select TO_CHAR(CURRENT_DATE, 'DD.MM.YYYY'), _manuf, _sheet_name, po_item,
  array_length(string_to_array(kks, E'\n'), 1) as kks_num,
  coalesce(qty_of_valves, qty_of_pumps) as qty
from (
  select 
    _created_at, _manuf, _sheet_name,
    jsonb_array_elements(content) ->> 'po_item' as po_item,
    jsonb_array_elements(content) ->> 'kks' as kks,
    jsonb_array_elements(content) ->> 'qty_of_valves' as qty_of_valves,
    jsonb_array_elements(content) ->> 'qty_of_pumps' as qty_of_pumps
  from (select *, max(_created_at) over(partition by _manuf, _sheet_name) as max_created_at from stage.schedules_values) as t1
  where _created_at = max_created_at
) as t2
where array_length(string_to_array(kks, E'\n'), 1) != try_cast_int(coalesce(qty_of_valves, qty_of_pumps))
  and kks not like '%-%'
  and kks not like '%NA%'
"""
    records = hook.get_records(KKS_VS_QTY_QUERY)
    if len(records) == 0:
        return None
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "manuf": r[0],
        "sheet_name": r[1],
        "po_item": r[2],
        "kks_num": r[3],
        "qty": r[4]
    } for r in records])
    return df
