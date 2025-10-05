with dim_ep as (
    select *
    from {{ ref('stg_episodes') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'episode_id',
        'episode_name',
        'episode_code',
        'episode_url'
    ]) }} as dim_episodes_sk,
    *
from dim_ep
