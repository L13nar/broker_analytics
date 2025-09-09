{{ config(materialized='table') }}

with dates as (
    select
        cast(date_day as date) as date,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        date_trunc('week', date_day) as week_start,
        extract(quarter from date_day) as quarter,
        extract(dow from date_day) as day_of_week
    from range(date '2020-01-01', date '2030-12-31', interval 1 day) as t(date_day)
)

select * from dates
