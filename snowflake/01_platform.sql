USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS dbt_pipe;

CREATE SCHEMA IF NOT EXISTS dbt_pipe.raw
    COMMENT = 'Batch landing zone. Iceberg on EV_ICEBERG.';
CREATE SCHEMA IF NOT EXISTS dbt_pipe.raw_streams
    COMMENT = 'Kafka landing tables, one per topic. Iceberg on EV_ICEBERG.';
CREATE SCHEMA IF NOT EXISTS dbt_pipe.control
    COMMENT = 'Deployed dbt project object and the scheduled build task.';

CREATE WAREHOUSE IF NOT EXISTS wh_transform
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 2
    COMMENT = 'Transform warehouse for the dbt tpch_pipeline project';

CREATE ROLE IF NOT EXISTS transformer
    COMMENT = 'dbt build role. Reads raw, owns bronze/silver/gold.';
CREATE ROLE IF NOT EXISTS kafka_ingest
    COMMENT = 'Write-only ingest role for the Snowflake Kafka connector.';

GRANT USAGE, CREATE SCHEMA ON DATABASE dbt_pipe TO ROLE transformer;
GRANT USAGE                ON DATABASE dbt_pipe TO ROLE kafka_ingest;
GRANT USAGE ON WAREHOUSE wh_transform TO ROLE transformer;
GRANT USAGE ON WAREHOUSE wh_transform TO ROLE kafka_ingest;
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake_sample_data TO ROLE transformer;

CREATE EXTERNAL VOLUME IF NOT EXISTS ev_iceberg
    STORAGE_LOCATIONS = (
        (
            NAME = 'iceberg-primary'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = '<% s3_base_url %>'
            STORAGE_AWS_ROLE_ARN = '<% aws_role_arn %>'
            STORAGE_AWS_EXTERNAL_ID = '<% aws_external_id %>'
        )
    )
    ALLOW_WRITES = TRUE
    COMMENT = 'Iceberg storage for the analytics raw layer';

GRANT USAGE ON EXTERNAL VOLUME ev_iceberg TO ROLE kafka_ingest;
GRANT USAGE ON EXTERNAL VOLUME ev_iceberg TO ROLE transformer;

GRANT USAGE ON SCHEMA dbt_pipe.raw TO ROLE transformer;
GRANT SELECT ON ALL    TABLES         IN SCHEMA dbt_pipe.raw TO ROLE transformer;
GRANT SELECT ON FUTURE TABLES         IN SCHEMA dbt_pipe.raw TO ROLE transformer;
GRANT SELECT ON ALL    ICEBERG TABLES IN SCHEMA dbt_pipe.raw TO ROLE transformer;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA dbt_pipe.raw TO ROLE transformer;

GRANT USAGE ON SCHEMA dbt_pipe.raw_streams TO ROLE kafka_ingest;
GRANT USAGE ON SCHEMA dbt_pipe.raw_streams TO ROLE transformer;
GRANT INSERT ON FUTURE ICEBERG TABLES IN SCHEMA dbt_pipe.raw_streams TO ROLE kafka_ingest;
GRANT INSERT ON ALL    ICEBERG TABLES IN SCHEMA dbt_pipe.raw_streams TO ROLE kafka_ingest;
GRANT CREATE PIPE  ON SCHEMA dbt_pipe.raw_streams TO ROLE kafka_ingest;
GRANT CREATE STAGE ON SCHEMA dbt_pipe.raw_streams TO ROLE kafka_ingest;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA dbt_pipe.raw_streams TO ROLE transformer;
GRANT SELECT ON ALL    ICEBERG TABLES IN SCHEMA dbt_pipe.raw_streams TO ROLE transformer;
GRANT SELECT ON FUTURE TABLES         IN SCHEMA dbt_pipe.raw_streams TO ROLE transformer;
GRANT SELECT ON ALL    TABLES         IN SCHEMA dbt_pipe.raw_streams TO ROLE transformer;

CREATE USER IF NOT EXISTS kafka_ingest_svc
    TYPE              = SERVICE
    DEFAULT_ROLE      = kafka_ingest
    DEFAULT_WAREHOUSE = wh_transform
    COMMENT           = 'Service account for the Snowflake Kafka connector.';

GRANT ROLE kafka_ingest TO USER kafka_ingest_svc;

ALTER USER kafka_ingest_svc SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhKDTydFreNEd2RxmVoO5ieMGImASrjKVz11DLitxzeimrOEu2d1ldNqS/8pvyTruPDUo9G2cpE6N0OYialgVPF/n16eGw5pUlZEruW+GdW6/dFjtrzGkeNQeWsz7629AclgbR8i4k8RDFvqc78cZpcDlAHv+0W9O8v2AFCAglRcQlsngQ0CFmNrF414yNOiHzZKkocjCicJ78NcNAgnLPymdWjJGcssnckd6jzc7Tmj3FcQstzGWGLC3I8JgUV4cR0mjhfXFspoAOFENmtWRwRVnf4hGLkj7QHFWrVLQAkYwxutL9x8ZSm3Bksxm35YtF4n1QrNZX9MK8hxB+rLCiQIDAQAB';

CREATE OR REPLACE VIEW dbt_pipe.raw_streams.landing_health AS
SELECT
    table_schema,
    table_name,
    row_count,
    bytes,
    last_altered                                          AS last_write_at,
    DATEDIFF('minute', last_altered, CURRENT_TIMESTAMP()) AS minutes_since_write,
    is_iceberg
FROM dbt_pipe.information_schema.tables
WHERE table_schema IN ('RAW_STREAMS', 'RAW')
  AND table_name LIKE '%_STREAM'
;

GRANT SELECT ON VIEW dbt_pipe.raw_streams.landing_health TO ROLE transformer;

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('EV_ICEBERG') AS volume_check;
SHOW SCHEMAS IN DATABASE dbt_pipe;
SHOW FUTURE GRANTS IN SCHEMA dbt_pipe.raw_streams;
