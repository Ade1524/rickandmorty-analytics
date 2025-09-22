with base as (


    select
        cast("ID" as integer)           as episode_id,
        cast("NAME" as varchar)         as episode_name,
        cast("AIR_DATE" as varchar)     as air_date,
        cast("EPISODE_CODE" as varchar)      as episode_code,   -- careful: column might be "EPISODE" not "EPISODE_CODE"
        cast("CHARACTERS_COUNT" as int)  as character_count,
        cast("CHARACTERS_URLS" as varchar)  as character_urls,
        cast("URL" as varchar)          as url,
        cast("CREATED" as timestamp)    as created
    from {{ ref('episodes') }}

)

select * from base
