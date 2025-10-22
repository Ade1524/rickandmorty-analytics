select
    l.location_name,
    count(distinct e.episode_id) as episode_count,
    count(distinct c.character_id) as character_count
from {{ ref('dim_location') }} l
join {{ ref('dim_character') }} c on c.location_name = l.location_name
join {{ ref('fact_character_episode_location') }} b on c.dim_character_key = b.dim_character_key
join {{ ref('dim_episode') }} e on e.dim_episode_key = b.dim_episode_key    
group by l.location_name
order by episode_count desc, character_count desc
