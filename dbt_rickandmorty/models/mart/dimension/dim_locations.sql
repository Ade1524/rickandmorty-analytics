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
    *
from dim_loc
