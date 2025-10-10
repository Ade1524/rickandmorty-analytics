with spe as (
        select
            species,
            count(distinct dim_character_sk) as character_count
        from {{ ref('dim_characters') }}
        group by species
        order by character_count desc
)

select * from spe