{{
    config(
        target_lag = 'DOWNSTREAM'
    )
}}

with source as (

    select * from {{ source('raw', 'nation_raw') }}

),

renamed as (

    select
        n_nationkey   as nation_key,
        n_name        as nation_name,
        n_regionkey   as region_key,
        _loaded_at

    from source

    qualify row_number() over (
        partition by n_nationkey
        order by _loaded_at desc
    ) = 1

)

select * from renamed
