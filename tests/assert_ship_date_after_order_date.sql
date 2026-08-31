select
    line_items.order_key,
    line_items.line_number,
    orders.order_date,
    line_items.ship_date,
    datediff('day', orders.order_date, line_items.ship_date) as days_early

from {{ ref('silver_line_items') }} as line_items
inner join {{ ref('silver_orders') }} as orders
    on line_items.order_key = orders.order_key

where line_items.ship_date < orders.order_date
