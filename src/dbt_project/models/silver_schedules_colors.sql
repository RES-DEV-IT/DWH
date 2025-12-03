{{ 
	config(
		materialized='incremental',
		schema='silver'
	) 
}}

select _created_at, _sheet_name,
    jsonb_array_elements(content) ->> 'wo' as wo
from stage.schedules_colors

{% if is_incremental() %}
	where _created_at > (select max(_created_at) from {{this}})
{% endif %}