{{
    config(
        materialized = 'incremental',
        unique_key   = 'payment_key',
        incremental_strategy = 'merge'
    )
}}

-- depends_on: {{ ref('stream_config') }}
-- depends_on: {{ ref('stream_column_config') }}
-- depends_on: {{ ref('bronze_payments') }}

{{ build_silver_stream('payments') }}
