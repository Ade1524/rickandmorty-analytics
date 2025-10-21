with exploded as (
    select
        c.dim_character_sk,
        c.total_episodes_feature,
        c.character_id,
        c.character_name,
        c.status,
        c.species,
        c.type,
        c.gender,
        character_day_created,
        t.value as episode_url,
        row_number() over (partition by c.character_id order by t.value) as rn
    from {{ ref('dim_characters') }} c,
         table(split_to_table(c.episodes_feature, '|')) as t
)
, character_episode as (
select
    a.dim_character_sk,
    b.dim_episodes_sk,
    c.dim_date_sk,
    a.character_id,
    a.total_episodes_feature,
    a.status,
    count_if(a.status = 'Alive') as alive_count,
    count_if(a.status = 'Dead') as death_count,
    a.character_day_created,
    b.episode_url,
    b.episode_id,
    b.seasons,
    b.episode_code,
    b.total_character_featuring,
    b.air_date,
    c.date_day
from exploded a 
left join {{ ref('dim_episodes') }} b
on a.episode_url = b.episode_url
left join {{ ref('dim_date') }} c
on b.air_date = c.date_day
group by
    a.dim_character_sk,
    b.dim_episodes_sk,
    c.dim_date_sk,
    a.character_id,
    a.total_episodes_feature,
    a.status,
    a.character_day_created,
    b.episode_url,
    b.episode_id,
    b.total_character_featuring,
    b.seasons,
    b.episode_code,
    b.air_date,
    c.date_day
     
)


select 
     dim_character_sk,
     dim_episodes_sk,
     dim_date_sk as dim_air_date_sk,
     character_id,
     character_day_created,
     status,
     total_episodes_feature,
     episode_id,
     seasons,
     episode_code,
     total_character_featuring,
     air_date     
from character_episode
order by 
         episode_id,
         character_id asc