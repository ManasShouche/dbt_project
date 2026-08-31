{{
    config(
        target_lag = 'DOWNSTREAM'
    )
}}

-- DOWNSTREAM: dim_customers declares the real lag and this follows it.

with source as (

    select * from {{ source('raw', 'region_raw') }}

),

renamed as (

    select
        r_regionkey   as region_key,
        r_name        as region_name,
        _loaded_at

    from source

    -- Full load each time; without this a reload duplicates every row.
    qualify row_number() over (
        partition by r_regionkey
        order by _loaded_at desc
    ) = 1

)

select * from renamed
