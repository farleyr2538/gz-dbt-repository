select
    date_date
    , ROUND(SUM(ads_cost), 2) as ads_cost
    , ROUND(SUM(impression), 2) as impression
    , ROUND(SUM(click), 2) as click
from {{ ref('int_campaigns') }}
group by date_date
order by date_date desc