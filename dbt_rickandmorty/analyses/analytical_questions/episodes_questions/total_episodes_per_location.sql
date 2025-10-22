select
    l.location_name,
    count(distinct e.episode_id) as total_episodes
from {{ ref('dim_location') }} l
join {{ ref('fact_character_episode_location') }} c
    on c.location_id = l.location_id
join {{ ref('dim_episode') }} e
    on e.episode_id = c.episode_id
group by l.location_name
order by total_episodes desc
