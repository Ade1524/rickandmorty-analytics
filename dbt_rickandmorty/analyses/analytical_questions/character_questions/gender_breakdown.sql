with gen as (
        select
            gender,
            count(distinct dim_character_key) as character_count
        from {{ ref('dim_character') }}
        group by gender
        order by character_count desc
)

select * from gen