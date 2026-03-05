-- Monthly customer cohort analysis
-- Groups customers by their first purchase month, then tracks how many
-- came back in subsequent months

with first_purchase as (

    select
        customer_key,
        date_trunc('month', first_order_at) as cohort_month

    from marts.dim_customers
    where first_order_at is not null

),

monthly_activity as (

    select
        fo.customer_key,
        date_trunc('month', fo.order_purchase_at) as activity_month

    from marts.fact_orders fo
    where fo.order_status != 'canceled'
    group by fo.customer_key, date_trunc('month', fo.order_purchase_at)

),

cohort_data as (

    select
        fp.cohort_month,
        ma.activity_month,
        datediff('month', fp.cohort_month, ma.activity_month) as months_since_first,
        count(distinct fp.customer_key) as customer_count

    from first_purchase fp
    inner join monthly_activity ma
        on fp.customer_key = ma.customer_key

    group by fp.cohort_month, ma.activity_month,
             datediff('month', fp.cohort_month, ma.activity_month)

)

select
    cohort_month,
    months_since_first,
    customer_count

from cohort_data
where months_since_first between 0 and 12
order by cohort_month, months_since_first;
