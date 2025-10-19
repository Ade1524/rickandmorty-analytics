
with  first_appearance as (
    select
        dc.character_id,
        min(de.episode_id) as first_episode_id
    from {{ ref('fact_mart_rkandmy') }} f
    left join {{ ref('dim_characters') }} dc
        on f.dim_character_sk = dc.dim_character_sk
    left join {{ ref('dim_episodes') }} de
        on f.dim_episodes_sk = de.dim_episodes_sk
    group by dc.character_id
)

,new_character_counts as (
    select
        fa.first_episode_id as episode_id,
        count(distinct fa.character_id) as new_character_count
    from first_appearance fa
    group by fa.first_episode_id
)
select
    de.episode_id,
    de.episode_name,
    de.seasons,
    ncc.new_character_count,
    rank() over (order by ncc.new_character_count desc) as rank
from new_character_counts ncc
join {{ ref('dim_episodes') }} de
    on ncc.episode_id = de.episode_id
order by rank

