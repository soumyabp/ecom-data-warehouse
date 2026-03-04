with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

payments as (

    select
        order_id,
        sum(payment_value) as total_payment_value,
        max(payment_type)  as primary_payment_type,
        max(payment_installments) as max_installments

    from {{ source('raw', 'raw_order_payments') }}
    group by order_id

),

final as (

    select
        oi.order_id,
        oi.order_item_id,
        o.customer_id                           as customer_key,
        oi.product_id                           as product_key,
        cast(o.order_purchase_at as date)       as order_date_key,
        oi.seller_id,
        o.order_status,
        p.primary_payment_type                  as payment_type,
        p.max_installments                      as payment_installments,
        oi.price,
        oi.freight_value,
        oi.price + oi.freight_value             as item_total,
        o.order_purchase_at,
        o.order_approved_at,
        o.delivered_carrier_at,
        o.delivered_customer_at,
        o.estimated_delivery_at,
        case
            when o.delivered_customer_at is not null
                 and o.estimated_delivery_at is not null
            then datediff('day', o.estimated_delivery_at, o.delivered_customer_at)
        end                                     as delivery_delay_days

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join payments p
        on o.order_id = p.order_id

)

select * from final
