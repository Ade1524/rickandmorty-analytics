with dim_char as (
    select 
        *
    from {{ ref('dim_character') }}
)

, dim_loc as (
    select 
        *
    from {{ ref('dim_location') }}
)

, dim_epi as (
    select 
        *
    from {{ ref('dim_episode') }}
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
        dc.dim_character_key,
        dc.dim_character_created_date_key,
        dc.character_id,
        dc.total_episodes_feature,
        dc.character_day_created,
        l.dim_location_key,
        l.dim_location_created_date_key,
        l.location_id as last_location_id,
        f.location_id as origin_location_id,
        l.location_date_created,
        e.dim_episode_key,
        e.dim_air_date_key,
        e.dim_episode_date_created_key,
        e.episode_id,
        e.episode_code,
        e.air_date,
        e.episode_day_created
    from dim_char dc 
    left join dim_loc  l on dc.location_name = l.location_name
    left join dim_loc  f on dc.origin_location_name = f.location_name
    left join dim_show_date a on dc.dim_character_created_date_key = a.dim_date_key
    left join int_ch_ep  ice on dc.character_id = ice.character_id
    left join dim_epi e on ice.episode_id = e.episode_id
    left join dim_show_date b on l.dim_location_created_date_key = b.dim_date_key
    left join dim_show_date c on e.dim_air_date_key = c.dim_date_key
    left join dim_show_date d on e.dim_episode_date_created_key = d.dim_date_key
)

select * 
from fact
order by character_id