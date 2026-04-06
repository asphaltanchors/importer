/*
ABOUTME: DBT macros for safely referencing optional columns in raw source relations.
ABOUTME: Lets staging models tolerate sparse DLT loads where some nullable fields are absent.
*/

{% macro first_existing_column_or_null(relation, column_names, fallback_type='varchar') %}
    {% if execute %}
        {% set relation_columns = adapter.get_columns_in_relation(relation) %}
        {% set relation_column_names = relation_columns | map(attribute='name') | map('lower') | list %}
        {% set result = namespace(column_sql='null::' ~ fallback_type) %}

        {% for column_name in column_names %}
            {% if column_name | lower in relation_column_names %}
                {% set result.column_sql = adapter.quote(column_name) %}
            {% endif %}
        {% endfor %}

        {{ result.column_sql }}
    {% else %}
        null::{{ fallback_type }}
    {% endif %}
{% endmacro %}
