with characters as (
    select *
    from {{ ref('dim_character') }}
)

, inter_nested as (
    select *
    from {{ ref('int_char_single_episode') }}
)

, dates as (
    select *
    from {{ ref('dim_date') }}
)

,status_count as (
    select dim_character_key,
           dim_character_created_date_key,
           character_id,
           status,
           total_episodes_feature,
           count_if(status = 'Alive') as alive_count,
           count_if(status = 'Dead') as death_count,
           character_day_created
    from characters
    group by 
           dim_character_key,
           dim_character_created_date_key,
           character_id,
           status,
           total_episodes_feature,
           character_day_created
)


select 
       b.dim_character_key,
       b.dim_character_created_date_key,
       a.character_id,
       a.character_name,
       a.episode_id as first_episode_featured,
       b.status,
       b.alive_count,
       b.death_count,
       b.character_day_created
from inter_nested a
join status_count b on a.character_id = b.character_id
join dates c  on b.dim_character_created_date_key = c.dim_date_key
order by b.character_id