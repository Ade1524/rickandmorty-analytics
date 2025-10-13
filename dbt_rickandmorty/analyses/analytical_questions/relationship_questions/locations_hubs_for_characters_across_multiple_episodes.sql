select
    l.location_name,
    count(distinct e.episode_id) as episode_count,
    count(distinct c.character_id) as character_count
from {{ ref('dim_locations') }} l
join {{ ref('dim_characters') }} c
    on c.location_name = l.location_name
join {{ ref('fact_mart_rkandmy') }} b
    on c.dim_character_sk = b.dim_character_sk
join {{ ref('dim_episodes') }} e
    on e.dim_episodes_sk = b.dim_episodes_sk
group by l.location_name
order by episode_count desc, character_count desc
