with add_seasons as (
        select *,
               'Season ' || (SUBSTR(episode_code, 2, 2))::INTEGER AS seasons
        from  {{ ref('dim_episodes') }}       

)

select
    e.seasons,
    count(distinct b.character_id) as total_unique_characters
from add_seasons e
join {{ ref('fact_mart_rkandmy') }} b
    on e.episode_id = b.episode_id
group by e.seasons
order by e.seasons