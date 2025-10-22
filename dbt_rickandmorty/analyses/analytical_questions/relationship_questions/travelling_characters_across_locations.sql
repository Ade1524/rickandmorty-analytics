select
    c.character_name,
    count(distinct e.episode_id) as episodes_traveled
from {{ ref('dim_character') }} c
join {{ ref('fact_character_episode_location') }} b on c.character_id = b.character_id
join {{ ref('dim_episode') }} e on b.episode_id = e.episode_id
where true
      and c.origin_location_name <> c.location_name
group by c.character_name
order by episodes_traveled desc

