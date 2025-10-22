with base as (
    select
        dl.location_id,
        dl.location_name,
        count(distinct dc.character_id) as total_residents
    from {{ ref('fact_character_episode_location') }} f
    left join {{ ref('dim_character') }} dc on f.dim_character_key = dc.dim_character_key
    left join {{ ref('dim_location') }} dl on f.dim_location_key = dl.dim_location_key
    group by dl.location_id, dl.location_name
)
select
    location_id,
    location_name,
    total_residents,
    rank() over (order by total_residents desc) as rank
from base

