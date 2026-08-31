-- One row per (order_key, line_number). Composite grain, so neither column
-- is unique alone and a plain `unique` on either would fail on correct data.
--
-- Hand-written rather than dbt_utils.unique_combination_of_columns: this
-- project ships without packages so it can deploy on a trial account.
--
-- If this fires, suspect the delete+insert in silver_line_items -- a partial
-- delete would leave lines from two deliveries side by side.

select
    order_key,
    line_number,
    count(*) as copies

from {{ ref('silver_line_items') }}

group by order_key, line_number
having count(*) > 1
