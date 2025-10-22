-- {% macro cast_date_to_string(column_name, target_type='varchar(255)') %}
--     -- Macro to safely cast a date-like column to a string type.
--     -- This is often used for consistency or for transferring keys.
--     CAST({{ column_name }} AS {{ target_type }}) AS {{ column_name }}
-- {% endmacro %}


{% macro date_to_string(column_name, date_format='YYYY-MM-DD') %}
    {#- 
    Converts a date/timestamp column to a string with specified format
    
    Args:
        column_name (string): Name of the date column to convert
        date_format (string): Snowflake date format string (default: YYYY-MM-DD)
        
    Returns:
        SQL expression that converts the date to string
        
    Supported formats:
        - 'YYYY-MM-DD' (ISO format, default): 2024-01-15
        - 'DD/MM/YYYY' (UK format): 15/01/2024
        - 'MM/DD/YYYY' (US format): 01/15/2024
        - 'YYYY-MM-DD HH24:MI:SS' (with time): 2024-01-15 14:30:00
        - 'Month DD, YYYY' (readable): January 15, 2024
        - 'DD-Mon-YYYY' (short): 15-Jan-2024
    #}
    
    to_char({{ column_name }}, '{{ date_format }}')
    
{% endmacro %}