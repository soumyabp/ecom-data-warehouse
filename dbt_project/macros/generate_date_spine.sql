{% macro generate_date_spine(date_range_ref) %}

    {#
        Generates a continuous series of dates between the min and max order dates.
        Takes a CTE name that must have columns: min_date, max_date.
    #}

    select
        unnest(
            generate_series(
                (select min_date from {{ date_range_ref }}),
                (select max_date from {{ date_range_ref }}),
                interval '1 day'
            )
        )::date as date_day

{% endmacro %}
