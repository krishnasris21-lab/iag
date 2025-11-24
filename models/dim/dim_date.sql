{{ config(materialized='table') }}

with calendar as (
    select
        date_series::date as date_day
    from generate_series(
        date '2008-01-01',
        date '2030-12-31',
        interval 1 day
    ) as t(date_series)
)

select
    date_day,
    date_part('year',  date_day)                  as year,
    date_part('month', date_day)                  as month,
    date_part('day',   date_day)                  as day,
    date_trunc('month', date_day)::date           as month_start,
    strftime(date_day, '%Y-%m')                   as year_month
from calendar
