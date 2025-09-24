with rename as (

    {{ cast_and_rename(
        ref('locations'),
        {
            "id": {"dtype": "integer", "alias": "location_id"},
            "name": {"dtype": "varchar", "alias": "location_name"},
            "type": {"dtype": "varchar", "alias": "location_type"},
            "dimension": {"dtype": "varchar", "alias": "location_dimension"},
            "residents_count": {"dtype": "integer", "alias": "total_residents_at_location"},
            "residents_urls": {"dtype": "varchar", "alias": "character_url_at_the_location"},
            "url": {"dtype": "varchar", "alias": "location_url"},
            "created": {"dtype": "timestamp", "alias": "location_created_at"}
        }
    ) }}

)

select * from rename

