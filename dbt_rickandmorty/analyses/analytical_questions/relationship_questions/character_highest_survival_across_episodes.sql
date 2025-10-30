select
    c.character_name,
    count(distinct a.episode_id) as total_appearances,
    c.status
from {{ ref('fct_episode') }} a
left join {{ ref('fct_character_episode') }} b on a.dim_episode_key = b.dim_episode_key
left join {{ ref('dim_character') }} c on b.dim_character_key = c.dim_character_key
group by c.dim_character_key, c.character_name, c.status
order by total_appearances desc

