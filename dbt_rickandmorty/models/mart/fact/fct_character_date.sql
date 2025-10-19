with status_count as (
    select dim_character_sk,
           character_id,
           status,
           total_episodes_feature,
           count_if(status = 'Alive') as alive_count,
           count_if(status = 'Dead') as death_count,
           character_day_created
    from {{ ref('dim_characters') }}
    group by 
           dim_character_sk,
           character_id,
           status,
           total_episodes_feature,
           character_day_created
)


select 
       b.dim_character_sk,
       c.dim_date_sk,
       a.character_id,
       a.character_name,
       a.episode_id as first_episode_featured,
       b.status,
       b.alive_count,
       b.death_count,
       b.character_day_created
from {{ ref('int_char_single_episode') }} a
join status_count b on a.character_id = b.character_id
join {{ ref('dim_date') }} c  on b.character_day_created = c.date_day
order by b.character_id