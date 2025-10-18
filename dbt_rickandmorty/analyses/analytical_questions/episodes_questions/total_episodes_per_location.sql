select
    l.location_name,
    count(distinct e.episode_id) as total_episodes
from {{ ref('dim_locations') }} l
join {{ ref('fact_mart_rkandmy') }} c
    on c.location_id = l.location_id
join {{ ref('dim_episodes') }} e
    on e.episode_id = c.episode_id
group by l.location_name
order by total_episodes desc
