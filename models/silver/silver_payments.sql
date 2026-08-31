{{
    config(
        materialized = 'incremental',
        unique_key   = 'payment_key',
        incremental_strategy = 'merge'
    )
}}

-- The whole point of the framework: this stream needed no transformation
-- SQL. Which bronze model to read, which payload fields to extract, how to
-- cast them and what to deduplicate on are all rows in seeds/config/.
--
-- A flat payload, one row in and one row out, so build_silver_stream covers
-- it entirely. Compare silver_line_items, which stays hand-written because
-- exploding an array is a different grain.

-- depends_on: {{ ref('stream_config') }}
-- depends_on: {{ ref('stream_column_config') }}
-- depends_on: {{ ref('bronze_payments') }}

{{ build_silver_stream('payments') }}
