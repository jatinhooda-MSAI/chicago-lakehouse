with requests as (
    select *, coalesce(community_area, 0) as area
    from {{ ref('stg_311_requests') }}
    where not is_duplicate
),

opened as (
    select cast(created_at as date) as activity_date, area, sr_type, count(*) as requests_opened
    from requests
    where created_at >= cast('{{ var("start_date") }}' as date)
    group by all
),

closed as (
    select
        cast(closed_at as date) as activity_date,
        area,
        sr_type,
        count(*) as requests_closed,
        median(days_to_close) as median_days_to_close
    from requests
    where closed_at >= cast('{{ var("start_date") }}' as date)
    group by all
)

select
    activity_date,
    area as community_area,
    sr_type,
    coalesce(requests_opened, 0) as requests_opened,
    coalesce(requests_closed, 0) as requests_closed,
    round(median_days_to_close, 2) as median_days_to_close
from opened
full join closed using (activity_date, area, sr_type)
