select
    e.seasons,
    count(b.character_id) as total_unique_characters
from {{ ref('dim_episode') }} e
join {{ ref('fct_character') }} b on e.episode_id = b.first_episode_featured
group by e.seasons
order by e.seasons