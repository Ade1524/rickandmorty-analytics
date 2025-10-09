with species_by_dimension as (
    select
        dl.location_dimension,
        dc.species,
        count(distinct dc.character_id) as character_count
    from {{ ref('fact_mart_rkandmy') }} f
    left join {{ ref('dim_characters') }} dc
        on f.dim_character_sk = dc.dim_character_sk
    left join {{ ref('dim_locations') }} dl
        on f.dim_locations_sk = dl.dim_locations_sk
    where dc.species is not null
    group by dl.location_dimension, dc.species
),
ranked as (
    select
        location_dimension,
        species,
        character_count,
        rank() over (partition by location_dimension order by character_count desc) as rank
    from species_by_dimension
)
select
    location_dimension,
    species,
    character_count,
    rank
from ranked
where rank <= 5
order by location_dimension, rank
