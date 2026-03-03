with source as (

    select * from {{ source('raw', 'raw_products') }}

),

translations as (

    select * from {{ ref('product_category_name_translation') }}

),

cleaned as (

    select
        p.product_id,
        p.product_category_name          as product_category_name_pt,
        t.product_category_name_english  as product_category_name_en,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p.product_photos_qty,
        p.product_name_lenght            as product_name_length,
        p.product_description_lenght     as product_description_length

    from source p
    left join translations t
        on p.product_category_name = t.product_category_name
    where p.product_id is not null

)

select * from cleaned
