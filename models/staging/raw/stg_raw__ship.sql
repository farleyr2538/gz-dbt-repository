with 

source as (

    select * from {{ source('raw', 'ship') }}

),

renamed as (

    select
        orders_id,
        shipping_fee,
        logCost AS log_cost,
        SAFE_CAST(ship_cost AS FLOAT64) AS ship_cost

    from source

)

select * from renamed
