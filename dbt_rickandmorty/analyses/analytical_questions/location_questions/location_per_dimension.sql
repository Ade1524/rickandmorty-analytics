select
    location_dimension,
    count(distinct location_id) as total_locations
from {{ ref('dim_location') }}
group by location_dimension
order by total_locations desc
