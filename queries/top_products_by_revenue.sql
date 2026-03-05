-- Top 20 products by total revenue
-- Run against the marts schema after dbt run completes

select
    dp.product_category_name_en   as category,
    dp.product_key,
    count(*)                      as items_sold,
    round(sum(fo.price), 2)       as total_revenue,
    round(avg(fo.price), 2)       as avg_price

from marts.fact_orders fo
join marts.dim_products dp
    on fo.product_key = dp.product_key

where fo.order_status != 'canceled'

group by dp.product_category_name_en, dp.product_key
order by total_revenue desc
limit 20;
