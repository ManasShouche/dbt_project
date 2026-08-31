{{
    config(
        materialized = 'incremental',
        unique_key   = 'order_key',
        incremental_strategy = 'merge'
    )
}}

-- Order fact, one row per order, with line items rolled up onto it.
--
-- Incremental on ingested_at, the platform's arrival clock -- not order_date
-- and not the producer's _loaded_at. The filter asks "what is new to me"
-- rather than "what is recent in the world", so a January order that shows
-- up in a March load is still picked up.
--
-- merge on order_key means a re-loaded month updates in place instead of
-- duplicating, so the model is replay-safe end to end.

with orders as (

    select * from {{ ref('silver_orders') }}

    {% if is_incremental() %}
    -- Minus a lookback window, never a bare `>`: out-of-order arrival is
    -- normal, and the overlap costs a re-read the merge turns into a no-op.
    where ingested_at > (
        select dateadd(hour, -{{ var('late_arrival_hours') }}, max(ingested_at))
        from {{ this }}
    )
    {% endif %}

),

line_items as (

    select * from {{ ref('silver_line_items') }}

    {% if is_incremental() %}
    -- Without this, every incremental run scans all line items to aggregate
    -- rows it is about to throw away.
    where order_key in (select order_key from orders)
    {% endif %}

),

sla as (

    select * from {{ ref('sla_thresholds') }}

),

line_item_rollup as (

    select
        order_key,
        count(*)                                    as line_item_count,
        sum(quantity)                               as total_quantity,
        sum(extended_price)                         as gross_amount,
        sum(extended_price * (1 - discount))        as net_amount,
        -- Rounded per line, then summed -- not summed then rounded. TPC-H
        -- defines o_totalprice that way, and the other order drifts a few
        -- cents per order, failing the reconciliation test on arithmetic.
        sum(round(extended_price * (1 - discount) * (1 + tax_rate), 2)) as net_amount_with_tax,
        min(ship_date)                              as first_ship_date,
        max(ship_date)                              as last_ship_date,
        max(receipt_date)                           as last_receipt_date

    from line_items
    group by 1

),

final as (

    select
        orders.order_key,
        orders.customer_key,
        orders.order_date,
        orders.order_status,
        orders.order_priority,
        orders.total_price,
        orders.clerk_id,

        line_item_rollup.line_item_count,
        line_item_rollup.total_quantity,
        line_item_rollup.gross_amount,
        line_item_rollup.net_amount,
        line_item_rollup.net_amount_with_tax,
        line_item_rollup.gross_amount - line_item_rollup.net_amount as discount_amount,
        line_item_rollup.first_ship_date,
        line_item_rollup.last_ship_date,
        line_item_rollup.last_receipt_date,

        -- Days from order placed to last line shipped, against the seeded
        -- policy for this priority.
        datediff('day', orders.order_date, line_item_rollup.last_ship_date)
            as days_to_ship,
        sla.max_ship_days,
        datediff('day', orders.order_date, line_item_rollup.last_ship_date)
            <= sla.max_ship_days as is_within_ship_sla,

        -- _loaded_at is the business-facing arrival time; ingested_at is the
        -- watermark this model's own incremental filter reads back.
        orders._loaded_at,
        orders.ingested_at

    from orders
    -- inner join: an order with no line items is not a shippable order, and
    -- carrying it here would put nulls through every downstream measure.
    inner join line_item_rollup
        on orders.order_key = line_item_rollup.order_key
    left join sla
        on orders.order_priority = sla.order_priority

)

select * from final
