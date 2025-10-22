select
    l.location_name,
    l.location_dimension,
    c.species,
    count(distinct c.character_id) as species_count
from {{ ref('dim_character') }} c 
join {{ ref('dim_location') }} l
on c.location_name = l.location_name
group by l.location_name, l.location_dimension, c.species
order by species_count desc