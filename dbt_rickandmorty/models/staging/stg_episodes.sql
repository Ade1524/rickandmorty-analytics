with rename as (

    {{ cast_and_rename(
        ref('episodes'),
        {
            "id": {"dtype": "integer", "alias": "episode_id"},
            "name": {"dtype": "varchar", "alias": "episode_name"},
            "air_date": {"dtype": "varchar"},
            "episode_code": {"dtype": "varchar"},
            "characters_count": {"dtype": "integer"},
            "characters_urls": {"dtype": "varchar"},
            "url": {"dtype": "varchar"},
            "created": {"dtype": "timestamp"}
        }
    ) }}

)

select 
      episode_id,
      episode_name,
      TO_DATE(air_date, 'MMMM DD, YYYY') as air_date,
      episode_code,
      characters_count as total_character_featuring,
      characters_urls,
      url as episode_url,
      created as episode_time_created
from rename


