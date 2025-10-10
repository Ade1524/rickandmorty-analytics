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
    *
from dim_ch
