select
    c.location_name,
    c.location_dimension,
    l.species,
    count(distinct l.character_id) as species_count
from {{ ref('dim_location') }} c 
join {{ ref('dim_character') }} l on c.location_name = l.location_name
join {{ ref('dim_character') }} f on c.location_name = f.origin_location_name
group by c.location_name, c.location_dimension,        
         l.species
order by species_count desc