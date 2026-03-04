with customers as (

    select * from {{ ref('stg_customers') }}

),

order_stats as (

    select
        customer_id,
        min(order_purchase_at) as first_order_at,
        max(order_purchase_at) as last_order_at,
        count(distinct order_id) as total_orders

    from {{ ref('stg_orders') }}
    group by customer_id

),

final as (

    select
        c.customer_id          as customer_key,
        c.customer_unique_id,
        c.customer_zip_code,
        c.customer_city,
        c.customer_state,
        o.first_order_at,
        o.last_order_at,
        coalesce(o.total_orders, 0) as total_orders

    from customers c
    left join order_stats o
        on c.customer_id = o.customer_id

)

select * from final
