with species_status as (
    select
        a.dim_character_key,
        a.species,
        a.status,
        count(a.dim_character_key) as character_count
    from {{ ref('dim_character') }} a
    left join {{ ref('fct_character') }}  b on a.dim_character_key = b.dim_character_key
    where a.status in ('Alive','Dead')
    group by a.species, 
             a.status,
             a.dim_character_key
),

aggregated as (
    select
        a.species,
        sum(b.alive_count) as total_alive,
        sum(b.death_count) as total_dead
    from species_status a
    left join {{ ref('fct_character') }}  b on a.dim_character_key = b.dim_character_key
    group by species
)

select
    species,
    total_alive,
    total_dead,
    round(total_alive*1.0 / nullif(total_alive + total_dead,0),2) as survival_rate
from aggregated
order by survival_rate desc
