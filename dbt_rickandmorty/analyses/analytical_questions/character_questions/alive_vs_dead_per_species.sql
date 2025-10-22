select
    species,
    sum(case when status='Alive' then 1 else 0 end) as alive_count,
    sum(case when status='Dead' then 1 else 0 end) as dead_count
from {{ ref('dim_character') }}
group by species
order by alive_count desc