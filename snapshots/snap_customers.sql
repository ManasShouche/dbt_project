{% snapshot snap_customers %}

{{
    config(
        unique_key = 'customer_key',
        strategy   = 'check',
        check_cols = [
            'customer_name',
            'address',
            'phone_number',
            'account_balance',
            'market_segment'
        ],
        invalidate_hard_deletes = true
    )
}}

-- SCD2 history for customers, which arrive as a mutable full load with no
-- change log of their own.
--
-- `check` rather than `timestamp`: a full reload restamps _loaded_at on every
-- row, so a timestamp strategy would version all 150k customers on every run.
-- check_cols is deliberately narrow -- comment and _loaded_at are excluded,
-- because versioning on a free-text field nobody queries just inflates the
-- table.

select
    customer_key,
    customer_name,
    address,
    phone_number,
    account_balance,
    market_segment,
    nation_key
from {{ ref('silver_customers') }}

{% endsnapshot %}
