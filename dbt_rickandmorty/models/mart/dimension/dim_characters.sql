with dim_ch as (
    select *
    from {{ ref('stg_characters') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'character_id',
        'image_url',
        'character_url'
    ]) }} as dim_character_sk,
    character_id,
    character_name,
    status,
    species,
    type,
    gender,
    origin_location_name,
    location_name,
    image_url as image_character_url,
    total_episodes_feature,
    episodes_feature,
    character_url,
    character_created_at :: date as character_day_created
from dim_ch
