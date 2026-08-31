{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns'
    )
}}

-- Append-only log of what arrived. One payload column, expanded in silver.
--
-- Watermark is ingested_at (SnowflakeConnectorPushTime): platform-assigned,
-- so it always increases on re-delivery. The producer clocks (_loaded_at,
-- CreateTime) move backwards on replay and silently drop rows.
--
-- Append rather than merge: at-least-once duplicates are the evidence of
-- what was delivered. Silver collapses them.

with source as (

    select * from {{ source('raw_streams', 'orders_stream') }}

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

        -- Coalesced for the 500 rows that landed before RECORD_METADATA
        -- existed; without it their watermark is NULL and every filter
        -- below skips them.
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
