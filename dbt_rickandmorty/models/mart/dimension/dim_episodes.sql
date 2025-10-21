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
    ]) }} as dim_episodes_key,
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
    dim_episodes_key,
    {{ date_to_string('episode_day_created', 'YYYYMMDD') }} as dim_episode_date_created_key,
    {{ date_to_string('air_date', 'YYYYMMDD') }} as dim_air_date_key,
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