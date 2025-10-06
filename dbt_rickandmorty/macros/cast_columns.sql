
{% macro cast_and_rename(source_ref, column_map) %}
    select
    {% for col, props in column_map.items() %}
        cast({{ col }} as {{ props['dtype'] }}) as {{ props.get('alias', col) }}{% if not loop.last %},{% endif %}
    {% endfor %}
    from {{ source_ref }}
{% endmacro %}

