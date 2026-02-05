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

def fill_percent(hook):
    QUERY = """
-- === ПРОЦЕНТ ЗАПОЛНЕННОСТИ ПО ЗИ ===
with start_of_production_date_extracted as (
  select 
    _created_at,
    _manuf,
    _sheet_name,
    try_parse_date(jsonb_array_elements(content) ->> 'start_of_production_date') as start_of_production_date,
    content
  from (select *, max(_created_at) over(partition by _manuf, _sheet_name) max_created_at from stage.schedules_values) as t1
  where _created_at = max_created_at
), active_projects as (
  select *
  from start_of_production_date_extracted
  where start_of_production_date is not null
    and start_of_production_date > '2025-09-01'
), key_values_schedules as (
   select 
        _created_at,
        _manuf, 
        _sheet_name,
        kv.key,
        kv.value
    from active_projects,
    lateral jsonb_array_elements(content) as elem,
    lateral jsonb_each_text((elem - 'comments')::jsonb) as kv
)
select
    TO_CHAR(MAX(_created_at), 'DD.MM.YYYY') as _created_at,
    _manuf as manufacturer,
  concat(
    ((1 - count(*) filter(where value = '') / count(*)::decimal) * 100) :: INTEGER,
    '%'
  ) as fill_percent
from key_values_schedules
group by _manuf
order by fill_percent desc
"""
    records = hook.get_records(QUERY)
    if len(records) == 0:
        return None
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "_created_at": r[0],
        "manuf": r[1],
        "fill_percent": r[2],
    } for r in records])

    return df

def date_correctness(hook):
    QUERY = """
    -- === ЗАПОЛНЕННОСТЬ ДАТ - % ЗАПОЛНЕННОСТИ ===
with start_of_production_date_extracted as (
  select 
    _created_at,
    _manuf,
    _sheet_name,
    try_parse_date(jsonb_array_elements(content) ->> 'start_of_production_date') as start_of_production_date,
    content
  from (select *, max(_created_at) over(partition by _manuf, _sheet_name) max_created_at from stage.schedules_values) as t1
  where _created_at = max_created_at
    and _created_at > CURRENT_DATE - interval '1 day'
), active_projects as (
  select *
  from start_of_production_date_extracted
  where start_of_production_date is not null
), key_values_schedules as (
   select 
        _created_at,
        _manuf, 
        _sheet_name,
        kv.key,
        kv.value
    from active_projects,
    lateral jsonb_array_elements(content) as elem,
    lateral jsonb_each_text((elem)::jsonb) as kv
)
select 
  TO_CHAR(MAX(_created_at), 'DD.MM.YYYY') as _created_at,
  _manuf, 
  CONCAT(
    ((1 - count(*) filter(where try_parse_date(value) is null)::decimal / count(*) ) * 100)::INTEGER,
    '%'
  ) as fill_percent
from key_values_schedules
where value != 'NA'
  AND REPLACE(value, ' ', '') != ''
  AND key in (
  'ebonite_lining_start',
  'cage_cage_readiness',
  'disc_semi-finish_machining_start',
  'actuator_receipt_date',
  'wedge_delivery_deadline_(po_vendor-foundry)',
  'disc_pos_with_foundry_forgings_vendor_(date)',
  'end_connector_delivery_deadline_(po_with_foundry)',
  'kpd4_submission_date',
  'pos_with_foundry_(date)',
  'pos_with_foudnry_(date)',
  'manufacturing_clearance',
  'overlay',
  'casting_receipt_from_jsons',
  'semi-finish_machining_start',
  'other_components_receipt_date_(+machining)',
  'body_finish_machining_start',
  'qt_basic_parts_start',
  'valve_assembly_completed',
  'wedge_pos_with_foundry_forgings_vendor_(date)',
  'end_connector_semi-finish_machining_completed',
  'rm1_readiness',
  'end_connector_finish_machining_start',
  'seat_pos_with_foundry_forgings_vendor_(date)',
  'kpd2_submission_date',
  'packing_+_kpd5_completed',
  'final_kpd5_received',
  'bonnet_bonnet_readiness',
  'body_semi-finish_machining_completed',
  'finish_machining_completed',
  'kpd11_submission_date',
  'rfq_sent_by_res',
  'packing_inspection_completed',
  'cage_pos_with_foundry_forgings_vendor_(date)',
  'actuator_readiness_date',
  'readiness_of_spares',
  'motor_readiness_date',
  'body_pos_with_foundry_forgings_vendor_(date)',
  'qt_basic_parts_readiness_(+machining)',
  'wnf_semi-finish_machining_completed',
  'patterns_start',
  'bonnet_delivery_deadline_(po_vendor-foundry)',
  'kpd5_draft_submission_date',
  'wnf_finish_machining_start',
  'kpd3_submission_date',
  'seat_seat_readiness',
  'wnf_delivery_deadline_(po_with_foundry)',
  'cover_cover_readiness',
  'rm1_readiness_to_inspection',
  'end_connector_end_connector_readiness',
  'valve_assembly_start',
  'end_connector_pos_with_foundry_forgings_vendor_(date)',
  'flange_flange_readiness',
  'body_delivery_deadline_(po_with_foundry)',
  'purchase_of_coupling_date',
  'bonnet_pos_with_foundry_forgings_vendor_(date)',
  'cage_delivery_deadline_(po_vendor-foundry)',
  'tpi_inspection',
  'disc_delivery_deadline_(po_with_foundry)',
  'testing_completed',
  'shipment_date',
  'other_components_receipt_date',
  'qt_basic_parts_readiness',
  'assembly_with_actuator_completed',
  'wnf_finish_machining_completed',
  'target_date_from_res_(dispatch)',
  'welding_start',
  'disc_disc_readiness',
  'coupling_receipt_date',
  'res_inspection_for_hardfacing',
  'res_inspection_passed',
  'target_date_from_res_(inspection_readiness)',
  'body_delivery_deadline_(po_vendor-foundry)',
  'disc_finish_machining_completed',
  'rm1_start',
  'body_body_readiness',
  'finish_machining_start',
  'other_components_start',
  'fat_by_res',
  'cover_pos_with_foundry_forgings_vendor_(date)',
  'disc_finish_machining_start',
  'pos_with_vendors',
  'rm1_inspection_passed',
  'flange_delivery_deadline_(po_vendor-foundry)',
  'patterns_(molds)_start',
  'baseplate_readiness',
  'wnf_pos_with_foundry_forgings_vendor_(date)',
  'wnf_semi-finish_machining_start',
  'end_connector_semi-finish_machining_start',
  'overlaying_completed',
  'welding_completed',
  'wedge_wedge_readiness',
  'pump_assembly_completed',
  'body_finish_machining_completed',
  'dispatch_completed',
  'start_of_production_date',
  'wnf_wnf_readiness',
  'patterns_readiness',
  'seat_delivery_deadline_(po_vendor-foundry)',
  'final_painting',
  'readiness_to_res_inspection',
  'disc_semi-finish_machining_completed',
  'hardfacing_inspection_completed',
  'assembly_with_motor_completed',
  'pump_assembly_start',
  'disc_delivery_deadline_(po_vendor-foundry)',
  'motor_receipt_date',
  'patterns_(molds)_readiness',
  'overlaying_start',
  'ebonite_lining_complete',
  'testing_start',
  'semi-finish_machining_completed',
  'assembly_with_actuator_start',
  'assembly_with_motor_start',
  'flange_pos_with_foundry_forgings_vendor_(date)',
  'dispatch_clearance_date',
  'kpd12_submission_date',
  'body_semi-finish_machining_start',
  'end_connector_finish_machining_completed')
group by _manuf
order by fill_percent desc
"""

    records = hook.get_records(QUERY)
    if len(records) == 0:
        return None
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "_created_at": r[0],
        "manuf": r[1],
        "fill_percent": r[2],
    } for r in records])

    return df

def errors_in_date_type(hook):
    QUERY = """
    with start_of_production_date_extracted as (
  select 
    _created_at,
    _manuf,
    _sheet_name,
    try_parse_date(jsonb_array_elements(content) ->> 'start_of_production_date') as start_of_production_date,
    content
  from (select *, max(_created_at) over(partition by _manuf, _sheet_name) max_created_at from stage.schedules_values) as t1
  where _created_at = max_created_at
    and _created_at > CURRENT_DATE - interval '3 day'
), active_projects as (
  select *
  from start_of_production_date_extracted
  where start_of_production_date is not null
), key_values_schedules as (
   select 
        _created_at,
        _manuf, 
        _sheet_name,
        elem ->> 'po_item' as po_item,
        kv.key,
        kv.value
    from active_projects,
    lateral jsonb_array_elements(content) as elem,
    lateral jsonb_each_text((elem)::jsonb) as kv
)
select 
    TO_CHAR(_created_at, 'DD.MM.YYYY'),
    _manuf,
    _sheet_name,
    po_item,
    key,
    value
from key_values_schedules
where value != 'NA'
  and replace(value, ' ', '') != ''
  and try_parse_date(value) is null
  and key in (
  'ebonite_lining_start',
  'cage_cage_readiness',
  'disc_semi-finish_machining_start',
  'actuator_receipt_date',
  'wedge_delivery_deadline_(po_vendor-foundry)',
  'disc_pos_with_foundry_forgings_vendor_(date)',
  'end_connector_delivery_deadline_(po_with_foundry)',
  'kpd4_submission_date',
  'pos_with_foundry_(date)',
  'pos_with_foudnry_(date)',
  'manufacturing_clearance',
  'overlay',
  'casting_receipt_from_jsons',
  'semi-finish_machining_start',
  'other_components_receipt_date_(+machining)',
  'body_finish_machining_start',
  'qt_basic_parts_start',
  'valve_assembly_completed',
  'wedge_pos_with_foundry_forgings_vendor_(date)',
  'end_connector_semi-finish_machining_completed',
  'rm1_readiness',
  'end_connector_finish_machining_start',
  'seat_pos_with_foundry_forgings_vendor_(date)',
  'kpd2_submission_date',
  'packing_+_kpd5_completed',
  'final_kpd5_received',
  'bonnet_bonnet_readiness',
  'body_semi-finish_machining_completed',
  'finish_machining_completed',
  'kpd11_submission_date',
  'rfq_sent_by_res',
  'packing_inspection_completed',
  'cage_pos_with_foundry_forgings_vendor_(date)',
  'actuator_readiness_date',
  'readiness_of_spares',
  'motor_readiness_date',
  'body_pos_with_foundry_forgings_vendor_(date)',
  'qt_basic_parts_readiness_(+machining)',
  'wnf_semi-finish_machining_completed',
  'patterns_start',
  'bonnet_delivery_deadline_(po_vendor-foundry)',
  'kpd5_draft_submission_date',
  'wnf_finish_machining_start',
  'kpd3_submission_date',
  'seat_seat_readiness',
  'wnf_delivery_deadline_(po_with_foundry)',
  'cover_cover_readiness',
  'rm1_readiness_to_inspection',
  'end_connector_end_connector_readiness',
  'valve_assembly_start',
  'end_connector_pos_with_foundry_forgings_vendor_(date)',
  'flange_flange_readiness',
  'body_delivery_deadline_(po_with_foundry)',
  'purchase_of_coupling_date',
  'bonnet_pos_with_foundry_forgings_vendor_(date)',
  'cage_delivery_deadline_(po_vendor-foundry)',
  'tpi_inspection',
  'disc_delivery_deadline_(po_with_foundry)',
  'testing_completed',
  'shipment_date',
  'other_components_receipt_date',
  'qt_basic_parts_readiness',
  'assembly_with_actuator_completed',
  'wnf_finish_machining_completed',
  'target_date_from_res_(dispatch)',
  'welding_start',
  'disc_disc_readiness',
  'coupling_receipt_date',
  'res_inspection_for_hardfacing',
  'res_inspection_passed',
  'target_date_from_res_(inspection_readiness)',
  'body_delivery_deadline_(po_vendor-foundry)',
  'disc_finish_machining_completed',
  'rm1_start',
  'body_body_readiness',
  'finish_machining_start',
  'other_components_start',
  'fat_by_res',
  'cover_pos_with_foundry_forgings_vendor_(date)',
  'disc_finish_machining_start',
  'pos_with_vendors',
  'rm1_inspection_passed',
  'flange_delivery_deadline_(po_vendor-foundry)',
  'patterns_(molds)_start',
  'baseplate_readiness',
  'wnf_pos_with_foundry_forgings_vendor_(date)',
  'wnf_semi-finish_machining_start',
  'end_connector_semi-finish_machining_start',
  'overlaying_completed',
  'welding_completed',
  'wedge_wedge_readiness',
  'pump_assembly_completed',
  'body_finish_machining_completed',
  'dispatch_completed',
  'start_of_production_date',
  'wnf_wnf_readiness',
  'patterns_readiness',
  'seat_delivery_deadline_(po_vendor-foundry)',
  'final_painting',
  'readiness_to_res_inspection',
  'disc_semi-finish_machining_completed',
  'hardfacing_inspection_completed',
  'assembly_with_motor_completed',
  'pump_assembly_start',
  'disc_delivery_deadline_(po_vendor-foundry)',
  'motor_receipt_date',
  'patterns_(molds)_readiness',
  'overlaying_start',
  'ebonite_lining_complete',
  'testing_start',
  'semi-finish_machining_completed',
  'assembly_with_actuator_start',
  'assembly_with_motor_start',
  'flange_pos_with_foundry_forgings_vendor_(date)',
  'dispatch_clearance_date',
  'kpd12_submission_date',
  'body_semi-finish_machining_start',
  'end_connector_finish_machining_completed')
"""

    records = hook.get_records(QUERY)
    if len(records) == 0:
        return None
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "_created_at": r[0],
        "_manuf": r[1],
        "_sheet_name": r[2],
        "po_item": r[3],
        "key": r[4],
        "value": r[5]
    } for r in records])

    return df

def two_weeks(hook):
    QUERY = """
  -- === НЕ БЫЛО ОБНОВЛЕНИЙ БОЛЬШЕ ДВУХ НЕДЕЛЬ ===
WITH latest_schedules AS (
    SELECT DISTINCT ON (_manuf, _sheet_name)
        _manuf,
        _sheet_name,
        content,
        _created_at
    FROM stage.schedules_values
    ORDER BY _manuf, _sheet_name, _created_at DESC
),
prev_schedules AS (
    SELECT DISTINCT ON (_manuf, _sheet_name)
        _manuf,
        _sheet_name,
        content,
        _created_at
    FROM stage.schedules_values
    WHERE _created_at < CURRENT_TIMESTAMP - INTERVAL '14 days'
    ORDER BY _manuf, _sheet_name, _created_at DESC
),
latest_colors AS (
    SELECT DISTINCT ON (_manuf, _sheet_name)
        _manuf,
        _sheet_name,
        content,
        _created_at
    FROM stage.schedules_colors
    ORDER BY _manuf, _sheet_name, _created_at DESC
),
prev_colors AS (
    SELECT DISTINCT ON (_manuf, _sheet_name)
        _manuf,
        _sheet_name,
        content,
        _created_at
    FROM stage.schedules_colors
    WHERE _created_at < CURRENT_TIMESTAMP - INTERVAL '14 days'
    ORDER BY _manuf, _sheet_name, _created_at DESC
),
kv_current_schedules AS (
    SELECT 
        ls._manuf, 
        ls._sheet_name,
        kv.key,
        kv.value,
        elem ->> '_row_number' AS rn
    FROM latest_schedules ls,
    LATERAL jsonb_array_elements(ls.content) AS elem,
    LATERAL jsonb_each_text((elem - 'comments')::jsonb) AS kv
    WHERE (elem ->> 'status') = 'Live'
),
kv_prev_schedules AS (
    SELECT 
        ps._manuf, 
        ps._sheet_name,
        kv.key,
        kv.value,
        elem ->> '_row_number' AS rn
    FROM prev_schedules ps,
    LATERAL jsonb_array_elements(ps.content) AS elem,
    LATERAL jsonb_each_text((elem - 'comments')::jsonb) AS kv
    WHERE (elem ->> 'status') = 'Live'
),
kv_current_colors AS (
    SELECT 
        lc._manuf, 
        lc._sheet_name,
        kv.key,
        kv.value,
        elem ->> '_row_number' AS rn
    FROM latest_colors lc,
    LATERAL jsonb_array_elements(lc.content) AS elem,
    LATERAL jsonb_each_text((elem - 'comments')::jsonb) AS kv
),
kv_prev_colors AS (
    SELECT 
        pc._manuf, 
        pc._sheet_name,
        kv.key,
        kv.value,
        elem ->> '_row_number' AS rn
    FROM prev_colors pc,
    LATERAL jsonb_array_elements(pc.content) AS elem,
    LATERAL jsonb_each_text((elem - 'comments')::jsonb) AS kv
), sheets_with_changes as (
  SELECT 
      cs._manuf, 
      cs._sheet_name
  FROM kv_current_schedules cs
  INNER JOIN kv_prev_schedules ps 
      ON cs._manuf = ps._manuf 
      AND cs._sheet_name = ps._sheet_name 
      AND cs.rn = ps.rn 
      AND cs.key = ps.key
  INNER JOIN kv_current_colors cc 
      ON cs._manuf = cc._manuf 
      AND cs._sheet_name = cc._sheet_name 
      AND cs.rn = cc.rn 
      AND cs.key = cc.key
  INNER JOIN kv_prev_colors pc 
      ON cs._manuf = pc._manuf 
      AND cs._sheet_name = pc._sheet_name 
      AND cs.rn = pc.rn 
      AND cs.key = pc.key
  WHERE ps.value != cs.value
     OR (
          pc.value != cc.value
          AND cc.value != '(255, 255, 0)'
     )
  GROUP BY cs._manuf, cs._sheet_name
)
select l._manuf, l._sheet_name
from latest_schedules l
left join sheets_with_changes c on l._manuf = c._manuf and l._sheet_name = c._sheet_name
where c._manuf is null
"""

    records = hook.get_records(QUERY)
    if len(records) == 0:
        return None
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "_manuf": r[0],
        "_sheet_name": r[1]
    } for r in records])

    return df

def yellow_dates(hook):
    QUERY = """-- === ЖЕЛТЫЕ ДАТЫ ===
with key_values_schedules as (
    select 
        _manuf, 
        _sheet_name,
        --jsonb_array_elements(content) ->> '_row_number' as rn,
        kv.key,
        try_parse_date(kv.value) as value,
        elem ->> '_row_number' AS rn,
        elem ->> 'po_item' AS po_item
    from (select *, max(_created_at) over(partition by _manuf, _sheet_name) max_created_at from stage.schedules_values) as t1,
    lateral jsonb_array_elements(content) as elem,
    lateral jsonb_each_text((elem - 'comments')::jsonb) as kv
    where _created_at = max_created_at
      and try_parse_date(kv.value) is not null
      and _created_at > CURRENT_TIMESTAMP - interval '1 day'
), key_values_colors as (
    select 
        _manuf, 
        _sheet_name,
        --jsonb_array_elements(content) ->> '_row_number' as rn,
        kv.key,
        kv.value as value,
        elem ->> '_row_number' AS rn
    from (select *, max(_created_at) over(partition by _manuf, _sheet_name) max_created_at from stage.schedules_colors) as t1,
    lateral jsonb_array_elements(content) as elem,
    lateral jsonb_each_text((elem - 'comments')::jsonb) as kv
    where _created_at = max_created_at
)
select
  v._manuf, v._sheet_name, v.po_item, v.key, TO_CHAR(v.value, 'DD.MM.YYYY'), c.value
from key_values_schedules v
inner join key_values_colors c
on c._manuf = v._manuf and c._sheet_name = v._sheet_name and c.key = v.key and c.rn = v.rn 
where c.value = '(255, 255, 0)'
"""

    records = hook.get_records(QUERY)
    if len(records) == 0:
        return None
    
    # === Парсим данные ===
    df = pd.DataFrame([{
        "_manuf": r[0],
        "_sheet_name": r[1],
        "po_item": r[2],
        "key": r[3],
        "value": r[4]
    } for r in records])

    return df
