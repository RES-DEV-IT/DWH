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
	lot::integer
from (
	select
		_created_at,
		_sheet_name,
		(jsonb_array_elements(content) ->> '_row_number')::integer as _row_number,
		SPLIT_PART(jsonb_array_elements(content) ->> 'po_no', '-', 1) as project_no,
		jsonb_array_elements(content)->>'lot' as lot
	from stage.schedules_values
) as t1
where {{ is_positive_integer('lot') }}
	


{% if is_incremental() %}
	where _created_at > (select max(_created_at) from {{this}})
{% endif %}
