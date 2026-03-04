with products as (

    select * from {{ ref('stg_products') }}

),

final as (

    select
        product_id                as product_key,
        product_category_name_pt,
        coalesce(
            product_category_name_en,
            product_category_name_pt,
            'uncategorized'
        )                         as product_category_name_en,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        product_photos_qty

    from products

)

select * from final
