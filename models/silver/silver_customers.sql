{{
    config(
        target_lag = 'DOWNSTREAM'
    )
}}

-- DOWNSTREAM: dim_customers declares the real lag and this follows it, so
-- freshness is set in one place for the whole chain.

with source as (

    select * from {{ source('raw', 'customer_raw') }}

),

renamed as (

    select
        c_custkey      as customer_key,
        c_name         as customer_name,
        c_address      as address,
        c_nationkey    as nation_key,
        c_phone        as phone_number,
        c_acctbal      as account_balance,
        c_mktsegment   as market_segment,
        c_comment      as customer_comment,
        _loaded_at

    from source

    -- Loaded in full each time rather than batched, so a reload would
    -- duplicate every customer without this.
    qualify row_number() over (
        partition by c_custkey
        order by _loaded_at desc
    ) = 1

)

select * from renamed
