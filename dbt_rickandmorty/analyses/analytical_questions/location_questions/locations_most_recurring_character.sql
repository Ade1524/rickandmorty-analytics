with character_episode_counts as (
    select
        f.character_id,
        f.total_episodes_feature
    from {{ ref('fact_character_episode_location') }} f 
    left join {{ ref('dim_episode') }} de on f.dim_episode_key = de.dim_episode_key
    group by f.character_id, f.total_episodes_feature
    order by character_id,
            total_episodes_feature 
)



,recurring_characters as (
    select
        character_id
    from character_episode_counts
    where total_episodes_feature > 1    -- recurring means more than one appearance
)



,location_recurring_counts as (
    select
        dla.location_id,
        dla.location_name,
        count(rc.character_id) as recurring_character_count
    from recurring_characters rc
    join {{ ref('fact_character_episode_location') }} fc on fc.character_id = rc.character_id
    join {{ ref('dim_location') }} dla on fc.dim_location_key = dla.dim_location_key
    group by 
    dla.location_id, 
    dla.location_name
)
select
    location_id,
    location_name,
    recurring_character_count,
    rank() over (order by recurring_character_count desc) as rank
from location_recurring_counts
order by rank

