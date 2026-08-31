{{
    config(
        materialized        = 'dynamic_table',
        target_lag          = '1 hour',
        snowflake_warehouse = target.warehouse,
        refresh_mode        = 'INCREMENTAL'
    )
}}

-- Customer dimension. Flattens the nation and region lookups so downstream
-- consumers never have to know those tables exist.
--
-- This is the only model in the customer chain that declares a real lag; the
-- three staging models it reads use target_lag='DOWNSTREAM' to follow it.
--
-- refresh_mode is pinned to INCREMENTAL deliberately. Left to AUTO,
-- Snowflake calls this a complex query and picks FULL, rebuilding all 150k
-- rows every hour. It is two left joins with no aggregation, so incremental
-- is both possible and correct.

with customers as (

    select * from {{ ref('silver_customers') }}

),

nations as (

    select * from {{ ref('silver_nations') }}

),

regions as (

    select * from {{ ref('silver_regions') }}

),

final as (

    select
        customers.customer_key,
        customers.customer_name,
        customers.market_segment,
        customers.account_balance,
        customers.phone_number,
        nations.nation_key,
        nations.nation_name,
        regions.region_key,
        regions.region_name,
        customers._loaded_at

    -- Left joins: a customer with a broken nation_key still lands in the
    -- dimension rather than vanishing. The relationships test on staging is
    -- what tells you it happened.
    from customers
    left join nations
        on customers.nation_key = nations.nation_key
    left join regions
        on nations.region_key = regions.region_key

)

select * from final
