{#
    The reusable processing engine. One macro, every stream.

    A silver stream model is three lines:

        {{ config(materialized='incremental', unique_key='order_key',
                  incremental_strategy='merge') }}
        -- depends_on: {{ ref('stream_config') }}
        {{ build_silver_stream('orders') }}

    Everything else -- which bronze model to read, which fields to pull out
    of the payload, how to cast them, what to deduplicate on -- comes from
    the two config tables. See docs/streams.md.

    Generates, in this order:
      1. read bronze, filtered to `watermark > max(watermark) - lookback`
      2. expand the payload into typed columns, per stream_column_config
      3. deduplicate to one row per primary key, newest delivery wins

    Steps 1 and 3 are what make the lookback free: the window re-reads rows
    already present and the model's MERGE turns those into no-ops. Dedupe is
    required rather than defensive -- MERGE fails outright on duplicate
    source rows, and at-least-once delivery guarantees them.

    SCOPE: the common shape, a flat extract from one payload object, one row
    in and one row out. A payload carrying an array to explode is a different
    grain and needs its own model -- see silver_line_items.
#}

{% macro build_silver_stream(stream_name) %}

{%- if not execute -%}
{#- Parse time: no connection yet, so the config tables cannot be read. dbt
    only needs the ref()/source()/config() calls at this stage, and those are
    declared as `-- depends_on:` lines in the model file. -#}
select 1 as parse_time_placeholder
{%- else -%}

{%- set cfg     = get_stream_config(stream_name) -%}
{%- set columns = get_stream_columns(stream_name) -%}

{%- set payload   = cfg.get('payload_column', 'record_content') -%}
{%- set watermark = cfg.get('watermark_field', 'ingested_at') -%}
{%- set pk        = cfg.get('primary_key') -%}

with bronze as (

    select * from {{ ref(cfg.get('bronze_model')) }}

    {% if is_incremental() %}
    {#- Lookback window, never a bare `>`. Out-of-order arrival is normal:
        a partition retry, a connector restart, a rebalance. -#}
    where {{ watermark }} > (
        select dateadd(hour, -{{ var('late_arrival_hours') }}, max({{ watermark }}))
        from {{ this }}
    )
    {% endif %}

),

expanded as (

    select
        {%- for col in columns %}
        {{ payload }}:{{ col['source_field'] }}::{{ col['data_type'] }} as {{ col['target_field'] }}{{ "," if not loop.last }}
        {%- endfor %},

        {# Lineage carried from bronze, not from the payload. kafka_offset
           breaks ties between rows sharing a watermark. #}
        {{ watermark }},
        kafka_offset

    from bronze

),

deduplicated as (

    select
        {%- for col in columns %}
        {{ col['target_field'] }},
        {%- endfor %}
        {{ watermark }}

    from expanded

    {# NULLS LAST is load-bearing: Snowflake sorts NULLs first on DESC, so a
       row with missing metadata would outrank properly-stamped rows. #}
    qualify row_number() over (
        partition by {{ pk }}
        order by {{ watermark }}  desc nulls last,
                 kafka_offset     desc nulls last
    ) = 1

)

select * from deduplicated

{%- endif -%}
{% endmacro %}
