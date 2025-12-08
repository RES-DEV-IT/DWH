{{ 
	config(
		materialized='incremental',
		schema='intermidiate'
	) 
}}

select 
	_created_at,
	_sheet_name,
	_row_number,
	project_no,
	po_no,
	lot::integer,
	po_item,
	size::integer,
	qty::integer,
	priority,
	status
from (
	select
		_created_at,
		_sheet_name,
		(jsonb_array_elements(content) ->> '_row_number')::integer as _row_number,
		SPLIT_PART(jsonb_array_elements(content) ->> 'po_no', '-', 1) as project_no,
		jsonb_array_elements(content) ->> 'po_no' as po_no,
		jsonb_array_elements(content)->>'lot' as lot,
		jsonb_array_elements(content)->>'po_item' as po_item,
		jsonb_array_elements(content)->>'size' as size,
		SPLIT_PART(jsonb_array_elements(content)->>'qty_of_valves', '.', 1) as qty_of_valves,
		SPLIT_PART(jsonb_array_elements(content)->>'qty_of_pumps', '.', 1) as qty_of_pumps,
		
		LOWER(jsonb_array_elements(content)->>'priority') as priority,
		LOWER(jsonb_array_elements(content)->>'status') as status,
		jsonb_array_elements(content)->>'manufacturing_clearance' as manufacturing_clearance
	from stage.schedules_values
) as t1
where {{ is_project_no('project_no') }}
	and {{ is_positive_integer('lot') }}
	and {{ is_po_item('po_item') }}
	and {{ is_positive_integer('size') }}
	and {{ is_positive_integer('qty') }}
	and {{ is_priority_valid('priority') }}
	and {{ is_status_valid('status') }}
	and ( {{ is_status_valid('qty_of_valves') }} or {{ is_status_valid('qty_of_pumps') }})

{% if is_incremental() %}
	and _created_at > (select max(_created_at) from {{ this }})
{% endif %}
