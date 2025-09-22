with characters_all as (
    SELECT id::integer AS character_id,
        name::varchar AS character_name,
        status::varchar,
        species::varchar,
        type::varchar AS character_type,
        gender::varchar,
        origin_name::varchar,
        origin_url::varchar,
        location_name::varchar,
        location_url::varchar,
        image::varchar,
        episode_count::integer,
        url::varchar AS character_url,
        created::timestamp_ntz AS created_at
    FROM {{ ref('characters') }}
)
select *
from characters_all