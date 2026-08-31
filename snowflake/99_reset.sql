USE ROLE ACCOUNTADMIN;

ALTER TASK IF EXISTS dbt_pipe.control.t_detect_arrivals SUSPEND;

TRUNCATE TABLE IF EXISTS dbt_pipe.raw_streams.orders_stream;
TRUNCATE TABLE IF EXISTS dbt_pipe.raw_streams.payments_stream;

CREATE OR REPLACE STREAM dbt_pipe.raw_streams.orders_arrivals
    ON TABLE dbt_pipe.raw_streams.orders_stream
    SHOW_INITIAL_ROWS = FALSE;

CREATE OR REPLACE STREAM dbt_pipe.raw_streams.payments_arrivals
    ON TABLE dbt_pipe.raw_streams.payments_stream
    SHOW_INITIAL_ROWS = FALSE;

TRUNCATE TABLE IF EXISTS dbt_pipe.control.ingest_log;

DROP TABLE IF EXISTS dbt_pipe.raw.orders_stream;
DROP TABLE IF EXISTS dbt_pipe.control.orders_raw_native;
DROP PROCEDURE IF EXISTS dbt_pipe.raw.load_month(DATE, BOOLEAN);

DROP SCHEMA IF EXISTS dbt_pipe.dbt_manas_bronze    CASCADE;
DROP SCHEMA IF EXISTS dbt_pipe.dbt_manas_silver    CASCADE;
DROP SCHEMA IF EXISTS dbt_pipe.dbt_manas_gold      CASCADE;
DROP SCHEMA IF EXISTS dbt_pipe.dbt_manas_audit     CASCADE;
DROP SCHEMA IF EXISTS dbt_pipe.dbt_manas_seeds     CASCADE;
DROP SCHEMA IF EXISTS dbt_pipe.dbt_manas_snapshots CASCADE;

SELECT table_schema, table_name, row_count
FROM dbt_pipe.information_schema.tables
WHERE table_schema IN ('RAW', 'RAW_STREAMS', 'CONTROL')
ORDER BY table_schema, table_name;
