with dim_ep as (
    select *
    from {{ ref('stg_episodes') }}
)
, dim_final as (
    select
    {{ dbt_utils.generate_surrogate_key([
        'episode_id',
        'episode_name',
        'episode_code',
        'episode_url'
    ]) }} as dim_episode_key,
    episode_id,
    episode_name,
    air_date,
    episode_code,
    'Season ' || (SUBSTR(episode_code, 2, 2))::INTEGER AS seasons,
    total_character_featuring,
    characters_urls,
    episode_url,
    episode_time_created :: date as episode_day_created
    
from dim_ep

)

select 
    dim_episode_key,
    {{ timestamp_to_date_key('episode_day_created', 'dim_episode_date_created_key') }},
    {{ timestamp_to_date_key('air_date', 'dim_air_date_key') }},
    episode_id,
    episode_name,
    air_date,
    episode_code,
    seasons,
    total_character_featuring,
    characters_urls,
    episode_url,
    episode_day_created

from dim_final