select
    cast(area_numbe as integer) as community_area,
    community as community_area_name
from {{ source('bronze', 'community_areas') }}

union all

select 0, 'UNKNOWN'
