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
    from {{ ref('int_characters_episodes') }}
)

, fact as (
    select 
        dc.dim_character_sk,
        dc.character_id,
        dc.total_episodes_feature,
        l.dim_locations_sk,
        l.location_id,
        e.dim_episodes_sk,
        e.episode_id,
        e.episode_code
    from dim_char dc 
    left join dim_loc  l on dc.location_name = l.location_name
    left join int_ch_ep  ice on dc.character_id = ice.character_id
    left join dim_epi e on ice.episode_id_join_key = e.episode_id
)

select * from fact