with dim_char as (
    select 
        *
    from {{ ref('dim_characters') }}
)

, dim_loc as (
    select 
        *
    from {{ ref('dim_locations') }}
)

, dim_epi as (
    select 
        *
    from {{ ref('dim_episodes') }}
)

, int_ch_ep as (
    select 
        *
    from {{ ref('int_char_single_episode') }}
)

,  dim_show_date as (
    select 
        *
    from {{ ref('dim_date') }}
)

, fact as (
    select 
        dc.dim_character_sk,
        dc.character_id,
        dc.total_episodes_feature,
        dc.character_day_created,
        d.date_day,
        l.dim_locations_sk,
        l.location_id as last_location_id,
        f.location_id as origin_location_id,
        e.dim_episodes_sk,
        e.episode_id,
        e.episode_day_created,
        e.episode_code
    from dim_char dc 
    left join dim_loc  l on dc.location_name = l.location_name
    left join dim_loc  f on dc.origin_location_name = f.location_name
    left join int_ch_ep  ice on dc.character_id = ice.character_id
    left join dim_epi e on ice.episode_id = e.episode_id
    left join dim_show_date d on dc.character_day_created = d.date_day
)

select * 
from fact
order by character_id