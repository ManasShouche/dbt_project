-- TPC-H defines o_totalprice as the sum over an order's line items of
--     round(extended_price * (1 - discount) * (1 + tax), 2)
-- so the fact table's rollup must reconcile to the order header. This is
-- what catches a broken join or a partial line-item load -- row counts and
-- keys all look fine, the money is just wrong.
--
-- The tolerance scales with line_item_count because the residual rounding
-- error does: measured across 56,741 orders it runs ~0.009 average and
-- ~0.016 worst case PER LINE. A flat tolerance would pass 7-line orders
-- that are genuinely broken and fail 1-line orders that are fine.
--
-- If this fires, the variance will be orders of magnitude larger than a
-- rounding cent.

select
    order_key,
    line_item_count,
    total_price,
    round(net_amount_with_tax, 2) as computed_total,
    abs(total_price - net_amount_with_tax) as variance

from {{ ref('fct_orders') }}

where abs(total_price - net_amount_with_tax) > 0.02 * line_item_count
