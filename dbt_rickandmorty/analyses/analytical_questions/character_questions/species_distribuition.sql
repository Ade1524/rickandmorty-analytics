with spe as (
        select
            species,
            count(distinct dim_character_key) as character_count
        from {{ ref('dim_character') }}
        group by species
        order by character_count desc
)

select * from spe