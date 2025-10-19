with sta as (
    select
        status,
        count(dim_character_sk) as character_count
    from {{ ref('dim_characters') }}
    group by status
    order by character_count desc
)

select * from sta