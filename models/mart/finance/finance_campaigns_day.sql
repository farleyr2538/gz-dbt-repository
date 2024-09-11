select
    date_date
    , ROUND(operational_margin - ads_cost, 2) as ads_margin
    , average_basket
    , operational_margin
    , ads_cost
    , impression
    , click
    , finance.revenue
    , finance.nb_transactions
    , finance.quantity_products_sold
from {{ ref('int_campaigns_day') }} as campaigns
join {{ ref('finance_days') }} as finance
using (date_date)
order by date_date desc