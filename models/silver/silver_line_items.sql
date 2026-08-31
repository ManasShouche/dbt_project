{{
    config(
        materialized = 'incremental',
        unique_key   = 'order_key',
        incremental_strategy = 'delete+insert'
    )
}}

-- Silver: explode the nested line_items array into one row per line.
--
-- delete+insert, not merge, and unique_key is order_key alone even though
-- the grain is (order_key, line_number). A re-delivered order can carry
-- FEWER lines than the stored copy; merge only inserts and updates, so the
-- dropped lines would survive as orphans and overstate every downstream
-- revenue total. The unit of replacement is the order, which is the unit
-- the payload delivers.
--
-- Same watermark and lookback as silver_orders, so an order's header and
-- its lines come from the same delivery.

with bronze as (

    select * from {{ ref('bronze_orders') }}

    {% if is_incremental() %}
    where ingested_at > (
        select dateadd(hour, -{{ var('late_arrival_hours') }}, max(ingested_at))
        from {{ this }}
    )
    {% endif %}

),

latest_delivery as (

    -- Collapse duplicate deliveries BEFORE exploding; the other order
    -- multiplies every duplicate across all of its lines.
    select * from bronze
    qualify row_number() over (
        partition by record_content:order_key::number
        order by ingested_at  desc nulls last,
                 kafka_offset desc nulls last
    ) = 1

),

exploded as (

    select
        latest_delivery.record_content:order_key::number as order_key,

        -- On a structured ARRAY, `value` stays typed rather than degrading
        -- to VARIANT, so these casts are assertions rather than repairs.
        line.value:line_number::number              as line_number,
        line.value:part_key::number                 as part_key,
        line.value:supplier_key::number             as supplier_key,
        line.value:quantity::number                 as quantity,
        line.value:extended_price::number(38,2)     as extended_price,
        line.value:discount::number(38,2)           as discount,
        line.value:tax::number(38,2)                as tax_rate,
        line.value:return_flag::string              as return_flag,
        line.value:line_status::string              as line_status,
        line.value:ship_date::date                  as ship_date,
        line.value:commit_date::date                as commit_date,
        line.value:receipt_date::date               as receipt_date,
        line.value:ship_instruct::string            as ship_instructions,
        line.value:ship_mode::string                as ship_mode,

        -- The payload carries no per-line comment. Declared so the column
        -- exists with a known type rather than appearing later as a schema
        -- change.
        cast(null as varchar)                       as line_comment,

        latest_delivery.record_content:_loaded_at::timestamp_ntz as _loaded_at,
        latest_delivery.ingested_at

    from latest_delivery,
         lateral flatten(
             input => latest_delivery.record_content:line_items
         ) as line

)

select * from exploded
