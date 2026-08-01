{{ config(materialized='incremental', unique_key='crime_id', incremental_strategy='delete+insert', on_schema_change='fail') }}

with changed as (
    select
        cast(id as bigint) as crime_id,
        case_number,
        cast("date" as timestamp) as occurred_at,
        block,
        iucr,
        primary_type,
        description,
        location_description,
        arrest,
        domestic,
        beat,
        district,
        cast(ward as integer) as ward,
        nullif(cast(community_area as integer), 0) as community_area,
        fbi_code,
        cast(updated_on as timestamp) as updated_on
    from {{ source('bronze', 'crimes') }}
    {% if is_incremental() %}
    where cast(updated_on as timestamp) >= cast('{{ var("min_date") }}' as timestamp)
        and cast(updated_on as timestamp) < cast('{{ var("max_date") }}' as timestamp)
    {% endif %}
),

candidates as (
    select * from changed
    {% if is_incremental() %}
    union all
    select * from {{ this }} where crime_id in (select crime_id from changed)
    {% endif %}
)

select *
from candidates
qualify row_number() over (partition by crime_id order by updated_on desc) = 1
