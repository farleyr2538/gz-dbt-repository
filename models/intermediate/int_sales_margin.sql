with purchase_cost_table as (
    
    select
        sales.date_date
        , sales.revenue as revenue
        , product.purchase_price
        , sales.quantity as quantity
        , round(sales.quantity * product.purchase_price, 2) AS purchase_cost
    from {{ ref('stg_raw__sales') }} as sales
    join {{ ref('stg_raw__product') }} as product
    on product.products_id = sales.products_id

)

select
    *
    , round(revenue - purchase_cost, 2) AS margin
from purchase_cost_table