USE ROLE ACCOUNTADMIN;
USE WAREHOUSE wh_transform;

SHOW PIPES IN SCHEMA dbt_pipe.raw_streams;
SELECT "name", "kind", "is_snowflake_managed", "definition"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SHOW ICEBERG TABLES IN SCHEMA dbt_pipe.raw_streams;
SELECT "name", "catalog_name", "iceberg_table_type", "external_volume_name",
       "iceberg_table_format_version", "base_location"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SHOW TASKS IN SCHEMA dbt_pipe.control;
SELECT "name", "state", "schedule", "predecessors", "condition"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM dbt_pipe.control.ingest_log ORDER BY detected_at DESC LIMIT 20;

SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(dbt_pipe.information_schema.task_history())
WHERE name IN ('T_DETECT_ARRIVALS', 'T_RUN_DBT')
ORDER BY scheduled_time DESC
LIMIT 20;

SHOW DYNAMIC TABLES IN DATABASE dbt_pipe;
SELECT "name", "schema_name", "scheduling_state", "target_lag", "refresh_mode"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

USE ROLE transformer;

SELECT * FROM dbt_pipe.raw_streams.landing_health;

SELECT
    RECORD_METADATA:topic::string               AS topic,
    RECORD_METADATA:partition::int              AS kafka_partition,
    COUNT(*)                                    AS rows_landed,
    MIN(RECORD_METADATA:offset::bigint)         AS min_offset,
    MAX(RECORD_METADATA:offset::bigint)         AS max_offset,
    COUNT(*) = MAX(RECORD_METADATA:offset::bigint)
             - MIN(RECORD_METADATA:offset::bigint) + 1
                                                AS is_contiguous,
    MAX(TO_TIMESTAMP_NTZ(RECORD_METADATA:SnowflakeConnectorPushTime::bigint, 3))
                                                AS last_snowpipe_push_at
FROM dbt_pipe.raw_streams.orders_stream
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT
    RECORD_METADATA:topic::string               AS topic,
    RECORD_METADATA:partition::int              AS kafka_partition,
    COUNT(*)                                    AS rows_landed,
    MAX(TO_TIMESTAMP_NTZ(RECORD_METADATA:SnowflakeConnectorPushTime::bigint, 3))
                                                AS last_snowpipe_push_at
FROM dbt_pipe.raw_streams.payments_stream
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT table_schema, table_name, row_count
FROM dbt_pipe.information_schema.tables
WHERE table_schema LIKE 'DBT\\_MANAS\\_%' ESCAPE '\\'
ORDER BY table_schema, table_name;

SELECT * FROM dbt_pipe.dbt_manas_audit.stream_audit;
