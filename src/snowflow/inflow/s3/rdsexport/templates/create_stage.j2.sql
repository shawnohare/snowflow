create or replace temporary stage {{ snowflake.database }}.{{ snowflake.schema }}.rds_import_{{ snowflake.schema }}_stage
  storage_integration = {{ snowflake.aws_integration }}
  file_format = ( type = PARQUET )
  url = 's3://{{ s3.path }}';
