with aggregated_order as (
    
    select
        orders_id
        , MIN(date_date) as date_date
        , round(SUM(revenue), 2) as revenue
        , SUM(quantity) as quantity
        , round(SUM(purchase_cost), 2) as purchase_cost
    from {{ ref('int_sales_margin') }}
    group by orders_id

)

select
    *
    , round(revenue - purchase_cost, 2) AS margin
from aggregated_order