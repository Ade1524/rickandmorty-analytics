with death_char_seasons as (
    select
        e.seasons,
        count(distinct c.character_id) as death_count
    from {{ ref('dim_character') }} c
    join {{ ref('fact_character_episode_location') }} b on c.dim_character_key = b.dim_character_key
    join {{ ref('dim_episode') }} e on e.dim_episode_key = b.dim_episode_key
    where lower(c.status) = 'dead'
    group by e.seasons
    order by e.seasons
                 
)

select *
from death_char_seasons