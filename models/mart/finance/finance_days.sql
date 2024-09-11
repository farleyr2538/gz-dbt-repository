/* */

with aggregated_sales as (
    
    select 
        date_date
        , COUNT(DISTINCT orders_id) as nb_transactions
        , ROUND(SUM(revenue), 2) as revenue
        , ROUND(SAFE_DIVIDE(SUM(revenue), COUNT(DISTINCT orders_id)), 2) AS average_basket
        , ROUND(SUM(margin), 2) AS margin
    from {{ ref('int_sales_margin') }}
    group by date_date

), 

aggregated_op_margin as (

    select
        date_date
        , ROUND(SUM(ship_fee), 2) as ship_fee
        , ROUND(SUM(ship_cost), 2) as ship_cost
        , ROUND(SUM(log_cost), 2) as log_cost
        , ROUND(SUM(operational_margin), 2) AS operational_margin
    from {{ ref('int_orders_operational') }}
    group by date_date

),

orders_margin as (

    select
        date_date
        , ROUND(SUM(purchase_cost), 2) as purchase_cost
        , SUM(quantity) as quantity
    from {{ ref('int_orders_margin') }}
    group by date_date

),

joined_table as (

    select
        aggregated_sales.*
        , aggregated_op_margin.operational_margin as operational_margin
        , orders_margin.purchase_cost as purchase_cost
        , aggregated_op_margin.ship_fee as ship_fee
        , aggregated_op_margin.log_cost as log_cost
        , orders_margin.quantity as quantity_products_sold
    from aggregated_sales
    left join orders_margin
    using (date_date)
    left join aggregated_op_margin
    using (date_date)

)

select
    *
from joined_table
--where date_date > '2021-09-27'
order by date_date desc

/*

TEST

select
    COUNT(DISTINCT orders_id) as freq
from {{ ref('int_sales_margin') }}
where date_date = '2021-09-30'
*/