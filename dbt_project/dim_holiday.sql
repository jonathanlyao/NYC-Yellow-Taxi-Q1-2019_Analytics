{{ config(materialized='table', schema='core') }}

select *
from (
    values
        (date '2019-01-01', 'New Year''s Day', 'Federal'),
        (date '2019-01-21', 'Martin Luther King Jr. Day', 'Federal'),
        (date '2019-02-14', 'Valentine''s Day', 'Commercial'),
        (date '2019-02-18', 'Presidents'' Day', 'Federal')
) as t(holiday_date, holiday_name, holiday_type)