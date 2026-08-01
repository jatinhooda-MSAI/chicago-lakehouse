with crimes as (
    select
        cast(date_trunc('month', crime_date) as date) as month,
        community_area,
        sum(incidents) as incidents,
        sum(arrests) as arrests,
        sum(domestic_incidents) as domestic_incidents
    from {{ ref('fct_crimes_daily') }}
    group by all
),

requests as (
    select
        cast(date_trunc('month', activity_date) as date) as month,
        community_area,
        sum(requests_opened) as requests_opened,
        sum(requests_closed) as requests_closed
    from {{ ref('fct_311_daily') }}
    group by all
),

close_times as (
    select
        cast(date_trunc('month', closed_at) as date) as month,
        coalesce(community_area, 0) as community_area,
        median(days_to_close) as median_days_to_close
    from {{ ref('stg_311_requests') }}
    where closed_at >= cast('{{ var("start_date") }}' as date) and not is_duplicate
    group by all
)

select
    month,
    community_area,
    area.community_area_name,
    cast(coalesce(incidents, 0) as bigint) as incidents,
    cast(coalesce(arrests, 0) as bigint) as arrests,
    round(arrests / nullif(incidents, 0), 4) as arrest_rate,
    cast(coalesce(domestic_incidents, 0) as bigint) as domestic_incidents,
    cast(coalesce(requests_opened, 0) as bigint) as requests_opened,
    cast(coalesce(requests_closed, 0) as bigint) as requests_closed,
    round(median_days_to_close, 2) as median_days_to_close
from crimes
full join requests using (month, community_area)
full join close_times using (month, community_area)
join {{ ref('dim_community_area') }} as area using (community_area)
