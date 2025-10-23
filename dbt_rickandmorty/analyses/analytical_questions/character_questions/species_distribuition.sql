with spe as (
        select
            a.species,
            count(b.dim_character_key) as character_count
        from {{ ref('dim_character') }} a
        left join {{ ref('fct_character') }}  b on a.dim_character_key = b.dim_character_key
        group by a.species
        order by character_count desc
)

select * from spe