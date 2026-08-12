select b.sr_number
from {{ source('bronze', 'requests_311') }} as b
left join {{ ref('stg_311_requests') }} as s using (sr_number)
where s.sr_number is null or s.last_modified_at < cast(b.last_modified_date as timestamp)
