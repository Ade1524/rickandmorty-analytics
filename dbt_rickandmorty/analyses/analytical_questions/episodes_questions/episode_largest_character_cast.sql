with episode_cast_size as (
    select
        de.episode_id,
        de.episode_name,
        de.seasons,
        count(distinct dc.character_id) as character_count
    from {{ ref('fact_mart_rkandmy') }} f
    left join {{ ref('dim_episodes') }} de
        on f.dim_episodes_sk = de.dim_episodes_sk
    left join {{ ref('dim_characters') }} dc
        on f.dim_character_sk = dc.dim_character_sk
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
