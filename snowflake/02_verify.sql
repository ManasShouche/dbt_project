USE ROLE transformer;
USE WAREHOUSE wh_transform;
USE SCHEMA dbt_pipe.raw_streams;

SELECT table_schema, table_name, table_type, is_iceberg, row_count
FROM dbt_pipe.information_schema.tables
WHERE table_schema IN ('RAW', 'RAW_STREAMS')
ORDER BY table_schema, table_name;

SELECT * FROM dbt_pipe.raw_streams.landing_health;

SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('DBT_PIPE.RAW_STREAMS.ORDERS_STREAM');

SELECT
    RECORD_METADATA:topic::string               AS topic,
    RECORD_METADATA:partition::int              AS kafka_partition,
    COUNT(*)                                    AS rows_landed,
    MIN(RECORD_METADATA:offset::bigint)         AS min_offset,
    MAX(RECORD_METADATA:offset::bigint)         AS max_offset,
    COUNT(*) = MAX(RECORD_METADATA:offset::bigint)
             - MIN(RECORD_METADATA:offset::bigint) + 1
                                                AS is_contiguous
FROM orders_stream
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT
    RECORD_CONTENT:order_key::number AS order_key,
    COUNT(*)                         AS copies
FROM orders_stream
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY copies DESC
LIMIT 20;

SELECT
    MIN(TO_TIMESTAMP_NTZ(RECORD_METADATA:CreateTime::bigint, 3)) AS earliest_produced,
    MAX(TO_TIMESTAMP_NTZ(RECORD_METADATA:CreateTime::bigint, 3)) AS latest_produced,
    COUNT(*)                                                     AS total_rows
FROM orders_stream;

SHOW DYNAMIC TABLES IN DATABASE dbt_pipe;
SELECT "name", "schema_name", "scheduling_state", "target_lag"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

USE ROLE accountadmin;
SHOW CHANNELS IN TABLE dbt_pipe.raw_streams.orders_stream;
