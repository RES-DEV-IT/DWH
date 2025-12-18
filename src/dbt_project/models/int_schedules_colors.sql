{{ 
	config(
		materialized='incremental',
		schema='intermediate'
	) 
}}

select *
from stage.schedules_colors


{% if is_incremental() %}
	where _created_at > (select max(_created_at) from {{this}})
{% endif %}