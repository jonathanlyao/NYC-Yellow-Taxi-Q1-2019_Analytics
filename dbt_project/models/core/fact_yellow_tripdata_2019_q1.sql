{{ config(materialized='table', schema='core') }}

with base as (
  
  select
    vendor_id,
    pickup_ts,
    dropoff_ts,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee

    from raw.stg_yellow_tripdata_2019_q1
), 

enriched as (

  select 
      *, 
      date_trunc('day', pickup_ts) as service_date,

      -- duration in seconds / hours 

      extract(epoch from (dropoff_ts - pickup_ts)) as trip_duration_seconds,
      extract(epoch from (dropoff_ts - pickup_ts)) / 3600 as trip_duration_hours

      from base
), 

flags as (

  select 
    *,
    case when pickup_ts is null or dropoff_ts is null then false
         when dropoff_ts <= pickup_ts then false
         when trip_distance is null or trip_distance <= 0 then false
         when total_amount is null or total_amount < 0 then false
         when trip_duration_seconds is null or trip_duration_seconds <= 0 then false
         else true 
    end as is_valid_trip, 

    -- avg speed only meaningful for valid trips, otherwise set to null
    case 
          when trip_duration_hours > 0
           and trip_distance is not null
           and trip_distance > 0
           then trip_distance / trip_duration_hours
           else null
    end as avg_speed_mph

    from enriched
), 

final as (

  select 
         *, 

         case 
             when is_valid_trip = true
              and avg_speed_mph between 0.1 and 80
              then true
              else false
         end as is_clean_trip

  from flags 

)

select * from final
