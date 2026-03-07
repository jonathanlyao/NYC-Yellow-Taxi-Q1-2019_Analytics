{{ config(materialized='table', schema='mart') }}


With base as (
    select
        service_date,
        total_amount,
        trip_distance,
        trip_duration_hours,
        avg_speed_mph,
        is_valid_trip,
        is_clean_trip
    from raw_core.fact_yellow_tripdata_2019_q1

), 

agg as (
    select 
        service_date,
        count(*) as total_trips_daily,
        sum(total_amount) as total_revenue_daily,

        -- valid trips
        count(*) filter (where is_valid_trip) as valid_trips_daily,

        -- clean trips
        count(*) filter (where is_clean_trip) as clean_trips_daily,
        (count(*) filter (where is_clean_trip))::numeric / nullif(count(*),0) as pct_clean_trips_daily,

        -- speed: weighted by distance on clean trips only
        sum(trip_distance) filter (where is_clean_trip) as clean_miles_daily, 
        sum(trip_duration_hours) filter (where is_clean_trip) as clean_hours_daily,
        (sum(trip_distance) filter (where is_clean_trip)) / nullif(sum(trip_duration_hours) filter (where is_clean_trip), 0) as avg_speed_mph_weighted_clean,

        -- revenue per trip
        (sum(total_amount) / nullif(count(*),0)) as avg_revenue_per_trip_daily

    from base
    group by service_date

)

select * from agg
