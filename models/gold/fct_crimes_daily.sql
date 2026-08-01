select
    cast(occurred_at as date) as crime_date,
    coalesce(community_area, 0) as community_area,
    primary_type,
    count(*) as incidents,
    count(*) filter (where arrest) as arrests,
    count(*) filter (where domestic) as domestic_incidents
from {{ ref('stg_crimes') }}
where occurred_at >= cast('{{ var("start_date") }}' as date)
group by all
