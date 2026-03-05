-- Every order_item must reference an existing order
select
    oi.order_id,
    oi.order_item_id

from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id

where o.order_id is null
