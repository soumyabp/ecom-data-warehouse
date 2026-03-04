with date_range as (

    select
        min(cast(order_purchase_at as date)) as min_date,
        max(cast(order_purchase_at as date)) as max_date
    from {{ ref('stg_orders') }}

),

date_spine as (

    {{ generate_date_spine('date_range') }}

),

final as (

    select
        date_day                                        as date_key,
        extract(dow from date_day)                      as day_of_week,
        extract(day from date_day)                      as day_of_month,
        extract(month from date_day)                    as month,
        extract(quarter from date_day)                  as quarter,
        extract(year from date_day)                     as year,
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end                                             as is_weekend,
        to_char(date_day, 'FMMonth')                    as month_name,
        to_char(date_day, 'FMDay')                      as day_name

    from date_spine

)

select * from final
