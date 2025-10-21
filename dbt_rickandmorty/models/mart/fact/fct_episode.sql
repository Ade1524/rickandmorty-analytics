with epi_c as (
    select  
               dim_episodes_key,
               to_char(episode_day_created, 'YYYYMMDD')::number AS episode_created_date_key,
               to_char(air_date, 'YYYYMMDD')::number AS air_date_key, 
               episode_id,
               air_date,
               episode_code,
               total_character_featuring,
               episode_day_created      
    from {{ ref('dim_episodes') }} 
)

select 
    *
from epi_c 

