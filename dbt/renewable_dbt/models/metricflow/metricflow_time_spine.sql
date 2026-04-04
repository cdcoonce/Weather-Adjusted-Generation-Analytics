{{
    config(
        materialized='table',
    )
}}

with days as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2025-12-31' as date)",
    ) }}
)

select
    cast(date_day as date) as date_day
from days
