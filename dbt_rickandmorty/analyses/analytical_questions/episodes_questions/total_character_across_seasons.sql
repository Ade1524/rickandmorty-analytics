select
    e.seasons,
    count(b.character_id) as total_unique_characters
from {{ ref('dim_episode') }} e
join {{ ref('fact_character_episode_location') }} b
    on e.episode_id = b.episode_id
group by e.seasons
order by e.seasons