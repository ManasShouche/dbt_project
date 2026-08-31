{{
    config(
        materialized = 'incremental',
        unique_key   = 'order_key',
        incremental_strategy = 'merge'
    )
}}

with orders as (

    select * from {{ ref('silver_orders') }}

    {% if is_incremental() %}

    where ingested_at > (
        select dateadd(hour, -{{ var('late_arrival_hours') }}, max(ingested_at))
        from {{ this }}
    )
    {% endif %}

),

line_items as (

    select * from {{ ref('silver_line_items') }}

    {% if is_incremental() %}

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

        datediff('day', orders.order_date, line_item_rollup.last_ship_date)
            as days_to_ship,
        sla.max_ship_days,
        datediff('day', orders.order_date, line_item_rollup.last_ship_date)
            <= sla.max_ship_days as is_within_ship_sla,

        orders._loaded_at,
        orders.ingested_at

    from orders

    inner join line_item_rollup
        on orders.order_key = line_item_rollup.order_key
    left join sla
        on orders.order_priority = sla.order_priority

)

select * from final
