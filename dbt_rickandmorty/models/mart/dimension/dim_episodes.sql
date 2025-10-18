with dim_ep as (
    select *
    from {{ ref('stg_episodes') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'episode_id',
        'episode_name',
        'episode_code',
        'episode_url'
    ]) }} as dim_episodes_sk,
    episode_id,
    episode_name,
    air_date,
    episode_code,
    total_character_featuring,
    characters_urls,
    episode_url,
    episode_time_created :: date as episode_day_created
    
from dim_ep
