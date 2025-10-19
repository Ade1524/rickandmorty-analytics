with episode_cast as (
    select
        de.episode_id,
        de.seasons,
        count(distinct dc.character_id) as character_count
    from {{ ref('fact_mart_rkandmy') }} f
    left join {{ ref('dim_episodes') }} de
        on f.dim_episodes_sk = de.dim_episodes_sk
    left join {{ ref('dim_characters') }} dc
        on f.dim_character_sk = dc.dim_character_sk
    group by de.episode_id, de.seasons
)
select
    seasons,
    count(distinct episode_id) as total_episodes,
    round(avg(character_count), 2) as avg_cast_size,
    max(character_count) as max_cast_size,
    min(character_count) as min_cast_size
from episode_cast
group by seasons
order by seasons
