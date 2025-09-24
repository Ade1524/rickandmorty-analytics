/*
{% macro cast_columns(source_ref, column_types) %}
    select
    {% for col, dtype in column_types.items() %}
        cast({{ col }} as {{ dtype }}) as {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
    from {{ source_ref }}
{% endmacro %}
*/
{% macro cast_and_rename(source_ref, column_map) %}
    select
    {% for col, props in column_map.items() %}
        cast({{ col }} as {{ props['dtype'] }}) as {{ props.get('alias', col) }}{% if not loop.last %},{% endif %}
    {% endfor %}
    from {{ source_ref }}
{% endmacro %}

