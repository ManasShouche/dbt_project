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
