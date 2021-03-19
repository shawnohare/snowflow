copy into {{ snowflake.database }}.{{ snowflake.schema }}.{{ snowflake.table }}
    from @{{ snowflake.database}}.{{ snowflake.schema}}.rds_import_{{ snowflake.schema }}_stage/{{ schema }}.{{ table }}
    pattern='.*[.]parquet'
    match_by_column_name = case_insensitive;
