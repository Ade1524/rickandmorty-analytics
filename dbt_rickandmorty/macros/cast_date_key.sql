{% macro timestamp_to_date_key(column_name, alias=none) %}
    {#-
    Converts a timestamp column to an integer date key in YYYYMMDD format
    
    Args:
        column_name (string): Name of the timestamp/date column
        alias (string): Optional alias for the output column
        
    Returns:
        Integer date key (e.g., 20240115 for January 15, 2024)
        
    Example:
        {{ timestamp_to_date_key('created_at', 'created_date_key') }}
        {{ timestamp_to_date_key('episode_day_created') }}
    #}
    
    {%- set output_alias = alias or (column_name ~ '_key') -%}
    
    cast(
        to_char(
            cast({{ column_name }} as date),
            'YYYYMMDD'
        ) as integer
    ) as {{ output_alias }}
    
{% endmacro %}