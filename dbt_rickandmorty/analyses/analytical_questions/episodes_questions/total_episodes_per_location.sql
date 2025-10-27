select
    l.location_name,
    count(distinct b.episode_id) as total_episodes
from {{ ref('dim_location') }} l
join {{ ref('fact_character_episode_location') }} b on b.last_location_id = l.location_id
group by l.location_name
order by total_episodes desc
