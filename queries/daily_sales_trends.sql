-- Daily and weekly sales trends
-- Uses the pre-aggregated fact_daily_sales table

-- Daily trend with 7-day moving average
select
    ds.sale_date_key,
    dd.day_name,
    dd.is_weekend,
    ds.total_orders,
    ds.total_revenue,
    ds.avg_order_value,
    round(
        avg(ds.total_revenue) over (
            order by ds.sale_date_key
            rows between 6 preceding and current row
        ),
        2
    ) as revenue_7d_avg

from marts.fact_daily_sales ds
join marts.dim_dates dd
    on ds.sale_date_key = dd.date_key

order by ds.sale_date_key;


-- Weekly summary
select
    dd.year,
    extract(week from ds.sale_date_key) as week_number,
    sum(ds.total_orders)                as weekly_orders,
    round(sum(ds.total_revenue), 2)     as weekly_revenue,
    round(avg(ds.avg_order_value), 2)   as avg_daily_aov

from marts.fact_daily_sales ds
join marts.dim_dates dd
    on ds.sale_date_key = dd.date_key

group by dd.year, extract(week from ds.sale_date_key)
order by dd.year, week_number;
