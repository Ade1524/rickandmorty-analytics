with epi_c as (
    select  
               a.episode_id, 
               a.episode_day_created,     
               b.dim_date_sk as dim_creation_episode_sk
        from RICKANDMORTY_DB.DBT_ADETAYO.DIM_EPISODES a
        left join RICKANDMORTY_DB.DBT_ADETAYO.DIM_DATE b on a.episode_day_created = b.date_day
)

, epi_a as  (
        select 
               a.episode_id, 
               a.air_date, 
               b.dim_date_sk as dim_air_date_sk
        from {{ ref('dim_episodes') }} a
        left join {{ ref('dim_date') }} b on a.air_date = b.date_day
)


, chiavi as (
    select 
        a.episode_id,
        a.dim_creation_episode_sk,
        b.dim_air_date_sk
    from epi_c a 
    left join epi_a b on a.episode_id = b.episode_id
)

, final as (
    select 
        a.*,
        b.dim_creation_episode_sk,
        b.dim_air_date_sk
    from {{ ref('dim_episodes') }} a 
    left join chiavi b on a.episode_id = b.episode_id
)

select 
    dim_episodes_sk,
    dim_creation_episode_sk,
    dim_air_date_sk,
    episode_id,
    air_date,
    episode_code,
    seasons,
    total_character_featuring,
    episode_day_created,      
from final
order by episode_id

