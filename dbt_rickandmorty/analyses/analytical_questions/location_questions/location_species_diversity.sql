with base as (
    select
        dl.location_dimension,
        dc.species
    from {{ ref('fact_character_episode_location') }} f
    left join {{ ref('dim_character') }} dc on f.dim_character_key = dc.dim_character_key
    left join {{ ref('dim_location') }} dl on f.dim_location_key = dl.dim_location_key
    where dc.species is not null
)
select
    location_dimension,
    count(distinct species) as unique_species_count
from base
group by location_dimension
order by unique_species_count desc
