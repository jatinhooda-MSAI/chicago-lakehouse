{{ config(meta={'dagster': {'ref': {'name': 'stg_crimes'}}}) }}

select b.id
from {{ source('bronze', 'crimes') }} as b
left join {{ ref('stg_crimes') }} as s on s.crime_id = cast(b.id as bigint)
where s.crime_id is null or s.updated_on < cast(b.updated_on as timestamp)
