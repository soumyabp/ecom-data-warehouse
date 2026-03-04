with daily as (

    select
        order_date_key                          as sale_date_key,
        count(distinct order_id)                as total_orders,
        count(*)                                as total_items,
        round(sum(price), 2)                    as total_revenue,
        round(sum(freight_value), 2)            as total_freight,
        round(sum(item_total), 2)               as total_gmv,
        round(avg(price), 2)                    as avg_item_price,
        round(
            sum(price) / nullif(count(distinct order_id), 0),
            2
        )                                       as avg_order_value

    from {{ ref('fact_orders') }}
    where order_status != 'canceled'
    group by order_date_key

)

select * from daily
