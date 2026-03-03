with source as (

    select * from {{ source('raw', 'raw_order_items') }}

),

cleaned as (

    select
        order_id,
        cast(order_item_id as integer)                   as order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp)           as shipping_limit_at,
        round(cast(price as decimal(12, 2)), 2)          as price,
        round(cast(freight_value as decimal(12, 2)), 2)  as freight_value

    from source
    where order_id is not null
      and product_id is not null

)

select * from cleaned
