select
    extract(month from date_date) as datemonth
    , ROUND(SUM(ads_margin), 2) AS ads_margin
    , ROUND(SUM(average_basket), 2) as average_basket
    --, ROUND(SAFE_DIVIDE(SUM(revenue), SUM(nb_transactions)), 2) AS average_basket
    , SUM(nb_transactions) as nb_transactions
    , ROUND(SUM(revenue), 2) as revenue
from {{ ref('finance_campaigns_day') }}
group by datemonth
order by datemonth desc