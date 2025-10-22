with episode_cast_size as (
    select
        de.episode_id,
        de.episode_name,
        de.seasons,
        count(distinct dc.character_id) as character_count
    from {{ ref('fact_character_episode_location') }} f
    left join {{ ref('dim_episode') }} de
        on f.dim_episode_key = de.dim_episode_key
    left join {{ ref('dim_character') }} dc
        on f.dim_character_key = dc.dim_character_key
    group by de.episode_id, de.episode_name, de.seasons
),
ranked as (
    select
        episode_id,
        episode_name,
        seasons,
        character_count,
        rank() over (order by character_count desc) as rank
    from episode_cast_size
)
select
    episode_id,
    episode_name,
    seasons,
    character_count,
    rank
from ranked
where rank <= 10
order by rank
