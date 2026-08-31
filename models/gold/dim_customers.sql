{{
    config(
        materialized        = 'dynamic_table',
        target_lag          = '1 hour',
        snowflake_warehouse = target.warehouse,
        refresh_mode        = 'INCREMENTAL'
    )
}}

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

    from customers
    left join nations
        on customers.nation_key = nations.nation_key
    left join regions
        on nations.region_key = regions.region_key

)

select * from final
