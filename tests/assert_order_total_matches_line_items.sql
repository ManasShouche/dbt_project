select
    order_key,
    line_item_count,
    total_price,
    round(net_amount_with_tax, 2) as computed_total,
    abs(total_price - net_amount_with_tax) as variance

from {{ ref('fct_orders') }}

where abs(total_price - net_amount_with_tax) > 0.02 * line_item_count
