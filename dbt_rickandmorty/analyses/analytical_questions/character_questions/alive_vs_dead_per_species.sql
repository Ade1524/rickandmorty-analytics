select
    a.species,
    sum(b.alive_count) as total_alive,
    sum(b.death_count) as total_dead
from {{ ref('dim_character') }} a
left join {{ ref('fct_character') }}  b on a.dim_character_key = b.dim_character_key
group by a.species
order by total_alive desc