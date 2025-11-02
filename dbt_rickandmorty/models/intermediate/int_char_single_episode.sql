{{ config(
    materialized = 'view'
) }}

with characters as (
    select *
    from {{ ref('stg_characters') }}
)

, episode as (
    select *
    from {{ ref('stg_episodes') }}
)
,unnested_charater_episode as (

    -- Step 1: Split the pipe-delimited episodes_feature into individual rows
    select
        c.character_id,
        c.character_name,
        c.episodes_feature,
        c.image_url,
        c.total_episodes_feature,
        c.character_url,
        t.index as episode_index,
        t.value as episode_url
    from characters as c,
         table(split_to_table(c.episodes_feature, '|')) as t

),

ranked as (

    -- Step 2: Rank each episode per character
    select
        e.*,
        row_number() over (partition by e.character_id order by e.episode_index) as rn
    from unnested_charater_episode e
)

-- Step 3: Keep only the first episode per character and join with episodes
select 
    r.character_id,
    r.character_name,
    r.character_url,
    e.episode_id,
    e.episode_name,
    e.episode_code,
    r.episode_url,
    r.total_episodes_feature
from ranked r
left join episode e
    on r.episode_url = e.episode_url
where r.rn = 1
order by r.character_id asc
