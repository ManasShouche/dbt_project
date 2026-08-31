USE ROLE ACCOUNTADMIN;

ALTER ACCOUNT SET DEFAULT_DBT_VERSION = '1.11.11';

CREATE TABLE IF NOT EXISTS dbt_pipe.control.ingest_log (
    stream_name  STRING,
    rows_arrived NUMBER,
    detected_at  TIMESTAMP_NTZ
);

CREATE STREAM IF NOT EXISTS dbt_pipe.raw_streams.orders_arrivals
    ON TABLE dbt_pipe.raw_streams.orders_stream
    SHOW_INITIAL_ROWS = FALSE;

CREATE STREAM IF NOT EXISTS dbt_pipe.raw_streams.payments_arrivals
    ON TABLE dbt_pipe.raw_streams.payments_stream
    SHOW_INITIAL_ROWS = FALSE;

CREATE OR REPLACE TASK dbt_pipe.control.t_detect_arrivals
    WAREHOUSE = wh_transform
    SCHEDULE  = '1 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    WHEN SYSTEM$STREAM_HAS_DATA('dbt_pipe.raw_streams.orders_arrivals')
      OR SYSTEM$STREAM_HAS_DATA('dbt_pipe.raw_streams.payments_arrivals')
AS
    INSERT INTO dbt_pipe.control.ingest_log (stream_name, rows_arrived, detected_at)
    SELECT 'orders',   COUNT(*), CURRENT_TIMESTAMP()
    FROM dbt_pipe.raw_streams.orders_arrivals
    UNION ALL
    SELECT 'payments', COUNT(*), CURRENT_TIMESTAMP()
    FROM dbt_pipe.raw_streams.payments_arrivals;

CREATE OR REPLACE TASK dbt_pipe.control.t_run_dbt
    WAREHOUSE = wh_transform
    AFTER dbt_pipe.control.t_detect_arrivals
AS
    EXECUTE DBT PROJECT dbt_pipe.control.tpch_clean
        ARGS = 'build --target dev';

ALTER TASK dbt_pipe.control.t_run_dbt RESUME;

SHOW TASKS IN SCHEMA dbt_pipe.control;

SELECT "name", "state", "schedule", "condition", "predecessors"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM dbt_pipe.control.ingest_log ORDER BY detected_at DESC LIMIT 20;

SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(dbt_pipe.information_schema.task_history())
ORDER BY scheduled_time DESC
LIMIT 20;
