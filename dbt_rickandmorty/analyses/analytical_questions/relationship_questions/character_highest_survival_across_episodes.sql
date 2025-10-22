select
    c.character_name,
    count(distinct b.episode_id) as total_appearances,
    c.status
from {{ ref('dim_character') }} c
left join {{ ref('fact_character_episode_location') }} b on c.dim_character_key = b.dim_character_key
group by c.character_name, c.status
order by total_appearances desc