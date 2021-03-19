create or replace table {{snowflake.database}}.{{snowflake.schema}}.{{ snowflake.table }}
(
    {% for column in table["columns"].values() -%}
    {{ column.name}} {{ column.snowflake_type}}{{ "," if not loop.last else "" }}
    {% endfor %}
)
comment = "Copied from RDS export {{ s3.path }}."
;
