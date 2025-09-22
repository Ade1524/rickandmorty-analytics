with location_all as (

SELECT
    id::integer AS location_id,
    name::varchar AS location_name,
    type::varchar AS location_type,
    dimension::varchar,
    residents_count::integer,
    residents_urls::varchar AS residents_url,
    url::varchar AS location_url,
    created::timestamp_ntz AS created_at
FROM {{ ref('locations') }}

)

select * from location_all