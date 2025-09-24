with rename as (

    {{ cast_and_rename(
        ref('characters'),
        {
            "id": {"dtype": "integer", "alias": "character_id"},
            "name": {"dtype": "varchar", "alias": "character_name"},
            "status": {"dtype": "varchar"},
            "species": {"dtype": "varchar"},
            "type": {"dtype": "varchar"},
            "gender": {"dtype": "varchar"},
            "origin_name": {"dtype": "varchar", "alias": "origin_location_name"},
            "origin_url": {"dtype": "varchar",  "alias": "origin_location_name_url"},
            "location_name": {"dtype": "varchar"},
            "location_url": {"dtype": "varchar"},
            "image": {"dtype": "varchar",  "alias": "image_url"},
            "episode_count": {"dtype": "integer", "alias": "total_episodes_feature"},
            "episode_urls": {"dtype": "varchar", "alias": "episodes_feature"},
            "url": {"dtype": "varchar", "alias": "character_url"},
            "created": {"dtype": "timestamp", "alias": "character_created_at"}
        }
    ) }}

)

select * from rename
