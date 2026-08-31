select
    order_key,
    line_number,
    discount

from {{ ref('silver_line_items') }}

where discount < 0
   or discount > 1
