with base as (
    select
        dl.location_id,
        dl.location_name,
        count(distinct dc.character_id) as total_residents
    from {{ ref('fact_mart_rkandmy') }} f
    left join {{ ref('dim_characters') }} dc on f.dim_character_sk = dc.dim_character_sk
    left join {{ ref('dim_locations') }} dl on f.dim_locations_sk = dl.dim_locations_sk
    group by dl.location_id, dl.location_name
)
select
    location_id,
    location_name,
    total_residents,
    rank() over (order by total_residents desc) as rank
from base

