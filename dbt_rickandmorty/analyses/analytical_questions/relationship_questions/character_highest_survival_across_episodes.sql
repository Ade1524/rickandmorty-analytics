select
    c.character_name,
    count(distinct b.episode_id) as total_appearances,
    c.status
from {{ ref('dim_characters') }} c
left join {{ ref('fact_mart_rkandmy') }} b
    on c.dim_character_sk = b.dim_character_sk
group by c.character_name, c.status
order by total_appearances desc