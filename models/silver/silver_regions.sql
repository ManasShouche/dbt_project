{{
    config(
        target_lag = 'DOWNSTREAM'
    )
}}

with source as (

    select * from {{ source('raw', 'region_raw') }}

),

renamed as (

    select
        r_regionkey   as region_key,
        r_name        as region_name,
        _loaded_at

    from source

    qualify row_number() over (
        partition by r_regionkey
        order by _loaded_at desc
    ) = 1

)

select * from renamed
