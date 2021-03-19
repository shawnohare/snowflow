create schema if not exists {{ snowflake.database }}.{{ snowflake.schema }}
COMMENT = "Import from RDS instance {{ instance }}.{{ schema }}";
