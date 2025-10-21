with dim_loc as (
    select *
    from {{ ref('stg_locations') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'location_id',
        'location_name',
        'location_url'
    ]) }} as dim_locations_sk,
    location_id,
    location_name,
    location_type,
    location_dimension,
    total_residents_at_location,
    character_url_at_the_location,
    location_url,
    location_created_at :: date as location_date_creat
from dim_loc
