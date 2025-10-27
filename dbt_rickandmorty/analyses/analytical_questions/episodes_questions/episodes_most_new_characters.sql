with new_character_counts as (
    select
        first_episode_featured as episode_id,
        count(distinct character_id) as new_character_count
    from {{ ref('fct_character') }} 
    group by first_episode_featured
    order by episode_id
)

select
    de.episode_id,
    de.episode_name,
    de.seasons,
    ncc.new_character_count,
    rank() over (order by ncc.new_character_count desc) as rank
from new_character_counts ncc
join {{ ref('dim_episode') }} de
    on ncc.episode_id = de.episode_id
order by rank
