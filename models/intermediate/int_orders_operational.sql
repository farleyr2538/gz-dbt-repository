select
    margin.orders_id as orders_id
    , margin.date_date as date_date
    , ship.shipping_fee as ship_fee
    , ship.ship_cost as ship_cost
    , ship.log_cost as log_cost
    , ROUND(margin.margin + ship.shipping_fee - ship.ship_cost - ship.log_cost, 2) AS operational_margin
from {{ ref('int_orders_margin') }} as margin
join {{ ref('stg_raw__ship') }} as ship
on margin.orders_id = ship.orders_id
--where margin.orders_id in (1002561, 1002560, 1002559)