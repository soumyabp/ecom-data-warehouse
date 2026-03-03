with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

cleaned as (

    select
        customer_id,
        customer_unique_id,
        lpad(cast(customer_zip_code_prefix as varchar), 5, '0') as customer_zip_code,
        lower(trim(customer_city))                               as customer_city,
        upper(trim(customer_state))                              as customer_state

    from source
    where customer_id is not null

),

-- keep one row per customer_id (there are no true duplicates in this table,
-- but this guards against any unexpected duplication in the raw extract)
deduplicated as (

    select
        *,
        row_number() over (partition by customer_id order by customer_unique_id) as rn

    from cleaned

)

select
    customer_id,
    customer_unique_id,
    customer_zip_code,
    customer_city,
    customer_state

from deduplicated
where rn = 1
