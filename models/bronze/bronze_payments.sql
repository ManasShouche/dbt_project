{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns'
    )
}}

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

        record_metadata:partition::number    as kafka_partition,
        record_metadata:offset::number       as kafka_offset,

        to_timestamp_ntz(
            record_metadata:CreateTime::bigint, 3
        )                                    as produced_at,

        coalesce(
            to_timestamp_ntz(record_metadata:SnowflakeConnectorPushTime::bigint, 3),
            record_content:_loaded_at::timestamp_ntz
        )                                    as ingested_at

    from source

)

select * from lineage

{% if is_incremental() %}

where not exists (
    select 1
    from {{ this }} as existing
    where existing.kafka_partition = lineage.kafka_partition
      and existing.kafka_offset    = lineage.kafka_offset
)
{% endif %}
