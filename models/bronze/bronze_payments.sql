{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns'
    )
}}

-- Append-only log of streamed payment deliveries. Same shape as
-- bronze_orders, and deliberately so -- adding a stream copies this file,
-- it does not invent a new pattern.
--
-- Watermark is ingested_at (SnowflakeConnectorPushTime): platform-assigned,
-- so it always increases on re-delivery. The producer clocks (payment_ts,
-- _loaded_at) move backwards on replay and silently drop rows.

with source as (

    select * from {{ source('raw_streams', 'payments_stream') }}

    {% if is_incremental() %}
    where coalesce(
              to_timestamp_ntz(record_metadata:SnowflakeConnectorPushTime::bigint, 3),
              record_content:_loaded_at::timestamp_ntz
          ) > (
              select dateadd(hour, -{{ var('late_arrival_hours') }}, max(ingested_at))
              from {{ this }}
          )
    {% endif %}

),

lineage as (

    select
        record_content,

        -- NUMBER, not STRING: as strings, offset "9" sorts above "100".
        record_metadata:partition::number    as kafka_partition,
        record_metadata:offset::number       as kafka_offset,

        to_timestamp_ntz(
            record_metadata:CreateTime::bigint, 3
        )                                    as produced_at,

        -- Defensive coalesce: this stream has no metadata-less legacy rows,
        -- but a NULL watermark would make a row invisible to every filter.
        coalesce(
            to_timestamp_ntz(record_metadata:SnowflakeConnectorPushTime::bigint, 3),
            record_content:_loaded_at::timestamp_ntz
        )                                    as ingested_at

    from source

)

select * from lineage

{% if is_incremental() %}
-- Append has no key to deduplicate on, so without this the lookback overlap
-- is inserted again on every run and bronze grows without bound.
where not exists (
    select 1
    from {{ this }} as existing
    where existing.kafka_partition = lineage.kafka_partition
      and existing.kafka_offset    = lineage.kafka_offset
)
{% endif %}
