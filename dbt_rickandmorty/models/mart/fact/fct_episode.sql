with epi_c as (
    select  
               dim_episode_key,
               to_char(episode_day_created, 'YYYYMMDD')::number AS episode_created_date_key,
               to_char(air_date, 'YYYYMMDD')::number AS air_date_key, 
               episode_id,
               air_date,
               episode_code,
               total_character_featuring,
               episode_day_created      
    from {{ ref('dim_episode') }} 
    
)

select 
    a.*
from epi_c a
left join {{ ref('dim_date') }} b on a.episode_created_date_key = b.dim_date_key 
left join {{ ref('dim_date') }} c on a.air_date_key = c.dim_date_key  


