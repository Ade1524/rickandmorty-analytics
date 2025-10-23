with gen as (
        select
            a.gender,
            count(b.dim_character_key) as character_count
        from {{ ref('dim_character') }} a
        left join {{ ref('fct_character') }}  b on a.dim_character_key = b.dim_character_key
        group by a.gender
        order by character_count desc
)

select * from gen

