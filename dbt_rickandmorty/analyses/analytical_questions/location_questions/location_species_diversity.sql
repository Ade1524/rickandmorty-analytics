with base as (
    select
        dl.location_dimension,
        dc.species
    from {{ ref('fact_mart_rkandmy') }} f
    left join {{ ref('dim_characters') }} dc on f.dim_character_sk = dc.dim_character_sk
    left join {{ ref('dim_locations') }} dl on f.dim_locations_sk = dl.dim_locations_sk
    where dc.species is not null
)
select
    location_dimension,
    count(distinct species) as unique_species_count
from base
group by location_dimension
order by unique_species_count desc
