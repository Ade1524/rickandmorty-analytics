with species_status as (
    select
        species,
        status,
        count(distinct dim_character_key) as character_count
    from {{ ref('dim_character') }}
    where status in ('Alive','Dead')
    group by species, status
),

aggregated as (
    select
        species,
        sum(case when status = 'Alive' then character_count else 0 end) as alive_count,
        sum(case when status = 'Dead' then character_count else 0 end) as dead_count
    from species_status
    group by species
)

select
    species,
    alive_count,
    dead_count,
    round(alive_count*1.0 / nullif(alive_count + dead_count,0),2) as survival_rate
from aggregated
order by survival_rate desc

