with dim_loc as (
    select *
    from {{ ref('stg_locations') }}
)


, final_dim as (

select
    {{ dbt_utils.generate_surrogate_key([
        'location_id',
        'location_name',
        'location_url'
    ]) }} as dim_location_key,
    location_id,
    location_name,
    location_type,
    location_dimension,
    total_residents_at_location,
    character_url_at_the_location,
    location_url,
    location_created_at :: date as location_date_created
from dim_loc

)

select 
    dim_location_key,
    {{ date_to_string('location_date_created', 'YYYYMMDD') }} as dim_location_created_date_key,
    location_id,
    location_name,
    location_type,
    location_dimension,
    total_residents_at_location,
    character_url_at_the_location,
    location_url,
    location_date_created
from final_dim