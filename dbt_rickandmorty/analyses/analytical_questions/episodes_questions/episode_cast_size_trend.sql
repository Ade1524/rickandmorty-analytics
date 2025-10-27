with episode_cast as (
    select
        de.episode_id,
        de.seasons,
        count(distinct dc.character_id) as character_count
    from {{ ref('fact_character_episode_location') }} f
    left join {{ ref('dim_episode') }} de on f.dim_episode_key = de.dim_episode_key
    left join {{ ref('dim_character') }} dc on f.dim_character_key = dc.dim_character_key
    group by de.episode_id, de.seasons
)
select
    seasons,
    count(episode_id) as total_episodes,
    round(avg(character_count), 2) as avg_cast_size,
    max(character_count) as max_cast_size,
    min(character_count) as min_cast_size
from episode_cast
group by seasons
order by seasons
