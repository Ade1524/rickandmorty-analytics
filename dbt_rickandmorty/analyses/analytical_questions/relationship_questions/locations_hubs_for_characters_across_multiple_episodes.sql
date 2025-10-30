select
    l.location_name,
    count(distinct e.episode_id) as episode_count,
    count(distinct b.character_id) as character_count
from {{ ref('dim_location') }} l
left join {{ ref('fact_character_episode_location') }} b on l.dim_location_key = b.dim_location_key
left join {{ ref('dim_episode') }} e on e.dim_episode_key = b.dim_episode_key    
group by l.location_name
order by episode_count desc, character_count desc
