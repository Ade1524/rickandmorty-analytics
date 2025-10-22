with sta as (
    select
        status,
        count(dim_character_key) as character_count
    from {{ ref('dim_character') }}
    group by status
    order by character_count desc
)

select * from sta