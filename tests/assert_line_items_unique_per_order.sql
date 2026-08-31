select
    order_key,
    line_number,
    count(*) as copies

from {{ ref('silver_line_items') }}

group by order_key, line_number
having count(*) > 1
