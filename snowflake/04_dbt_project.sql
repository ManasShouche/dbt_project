-- dbt version pin and EVENT-DRIVEN build. ACCOUNTADMIN. Idempotent.
--
-- v2 change: the build fires when data arrives, not on a wall clock. v1 ran
-- an hourly cron task that rebuilt whether or not anything had landed.
--
-- THE TRAP THIS DESIGN AVOIDS. A triggered task fires while its stream has
-- data, and a stream only advances when a DML statement consumes it.
-- EXECUTE DBT PROJECT reads the base tables, never the stream, so a task
-- that only runs dbt would leave the stream permanently unconsumed and fire
-- forever -- a warehouse that never suspends.
--
-- Hence two tasks: the root consumes the streams (which advances them) and
-- records the arrival; the child runs dbt after it.

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

-- SCHEDULE + WHEN, not a bare triggered task. The condition is evaluated
-- without a warehouse and costs nothing when false, so this is still
-- event-driven -- it builds only when data actually arrived. The schedule is
-- a rate floor: a full build takes ~18s, and a pure triggered task polls
-- every ~12s, so a continuously-fed topic would rebuild back to back and the
-- warehouse would never suspend.
--
-- For true trigger-on-arrival (~12s latency, only safe for bursty sources),
-- drop the SCHEDULE line.
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

-- No SUSPEND_TASK_AFTER_NUM_FAILURES here: it is a root-only parameter and
-- children inherit the root's setting. Setting it on a child is an error.
CREATE OR REPLACE TASK dbt_pipe.control.t_run_dbt
    WAREHOUSE = wh_transform
    AFTER dbt_pipe.control.t_detect_arrivals
AS
    EXECUTE DBT PROJECT dbt_pipe.control.tpch_clean
        ARGS = 'build --target dev';

-- Children resume before the root, or the graph will not run.
ALTER TASK dbt_pipe.control.t_run_dbt RESUME;
-- Root stays suspended. Resume it to arm the pipeline:
--     ALTER TASK dbt_pipe.control.t_detect_arrivals RESUME;

SHOW TASKS IN SCHEMA dbt_pipe.control;

SELECT "name", "state", "schedule", "condition", "predecessors"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT * FROM dbt_pipe.control.ingest_log ORDER BY detected_at DESC LIMIT 20;

SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(dbt_pipe.information_schema.task_history())
ORDER BY scheduled_time DESC
LIMIT 20;
