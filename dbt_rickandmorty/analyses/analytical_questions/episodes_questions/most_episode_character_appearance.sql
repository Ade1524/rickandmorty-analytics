with base as (
    select 
        character_name,
        total_episodes_feature
    from {{ ref('dim_character') }}
    WHERE true
        character_name NOT ILIKE '%rick%'
        AND character_name NOT ILIKE '%morty%'
    order by 
        total_episodes_feature desc
    limit 5
)

select * 
from base