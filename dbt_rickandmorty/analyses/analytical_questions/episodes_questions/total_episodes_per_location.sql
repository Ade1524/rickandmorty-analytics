select
    l.location_name,
    count(distinct e.episode_id) as total_episodes
from {{ ref('dim_locations') }} l
join {{ ref('dim_characters') }} c
    on c.location_name = l.location_name
join {{ ref('fact_mart_rkandmy') }} b
    on c.character_id = b.character_id
join {{ ref('dim_episodes') }} e
    on e.episode_id = b.episode_id
group by l.location_name
order by total_episodes desc
