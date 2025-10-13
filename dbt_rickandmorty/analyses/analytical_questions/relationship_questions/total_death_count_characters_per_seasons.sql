with add_seasons as (
        select *,
               'Season ' || (SUBSTR(episode_code, 2, 2))::INTEGER AS seasons
        from  {{ ref('dim_episodes') }}       

)

select
    e.seasons,
    count(distinct c.character_id) as death_count
from {{ ref('dim_characters') }} c
join {{ ref('fact_mart_rkandmy') }} b
    on c.dim_character_sk = b.dim_character_sk
join add_seasons e
    on e.dim_episodes_sk = b.dim_episodes_sk
where lower(c.status) = 'dead'
group by e.seasons
order by e.seasons
