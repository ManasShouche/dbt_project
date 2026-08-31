-- Discount is a fraction between 0 and 1, never a percentage.
--
-- Silent bug: a 7 that should have been 0.07 does not error anywhere, it
-- just makes every downstream revenue figure negative.

select
    order_key,
    line_number,
    discount

from {{ ref('silver_line_items') }}

where discount < 0
   or discount > 1
