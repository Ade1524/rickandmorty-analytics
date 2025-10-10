with gen as (
        select
            gender,
            count(distinct dim_character_sk) as character_count
        from {{ ref('dim_characters') }}
        group by gender
        order by character_count desc
)

select * from gen