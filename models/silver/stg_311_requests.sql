{{ config(materialized='incremental', unique_key='sr_number', incremental_strategy='delete+insert', on_schema_change='fail') }}

with changed as (
    select
        sr_number,
        sr_type,
        sr_short_code,
        owner_department,
        status,
        origin,
        cast(created_date as timestamp) as created_at,
        cast(closed_date as timestamp) as closed_at,
        cast(last_modified_date as timestamp) as last_modified_at,
        duplicate as is_duplicate,
        legacy_record as is_legacy,
        parent_sr_number,
        nullif(cast(community_area as integer), 0) as community_area,
        cast(ward as integer) as ward,
        round(date_diff('second', cast(created_date as timestamp), cast(closed_date as timestamp)) / 86400, 3) as days_to_close
    from {{ source('bronze', 'requests_311') }}
    {% if is_incremental() %}
    where cast(last_modified_date as timestamp) >= cast('{{ var("min_date") }}' as timestamp)
        and cast(last_modified_date as timestamp) < cast('{{ var("max_date") }}' as timestamp)
    {% endif %}
),

candidates as (
    select * from changed
    {% if is_incremental() %}
    union all
    select * from {{ this }} where sr_number in (select sr_number from changed)
    {% endif %}
)

select *
from candidates
qualify row_number() over (partition by sr_number order by last_modified_at desc) = 1
