with exploded as (
    select
        c.dim_character_key,
        dim_character_created_date_key,
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
    from {{ ref('dim_character') }} c,
         table(split_to_table(c.episodes_feature, '|')) as t
)
, character_episode as (
select
    a.dim_character_key,
    b.dim_episode_key,
    a.dim_character_created_date_key,
    b.dim_episode_date_created_key,
    b.dim_air_date_key,
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
    b.episode_day_created,
    c.date_day
from exploded a 
left join {{ ref('dim_episode') }} b on a.episode_url = b.episode_url
left join {{ ref('dim_date') }} c on b.dim_episode_date_created_key = c.dim_date_key
left join {{ ref('dim_date') }} d on b.dim_air_date_key = d.dim_date_key
group by
    a.dim_character_key,
    b.dim_episode_key,
    a.dim_character_created_date_key,
    b.dim_episode_date_created_key,
    b.dim_air_date_key,
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
    b.episode_day_created,
    c.date_day
     
)


select 
    dim_character_key,
    dim_episode_key,
    dim_character_created_date_key,
    dim_episode_date_created_key,
    dim_air_date_key,
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