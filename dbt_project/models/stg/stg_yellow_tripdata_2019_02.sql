with src as (

    select * 
    from {{ source('raw', 'yellow_tripdata_2019_01') }} 
    
),

typed as (

    select
        cast(vendor_id as integer) as vendor_id,
        cast(tpep_pickup_datetime as timestamp) as pickup_ts,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,

        cast(passenger_count as numeric) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        cast(ratecode_id as integer) as ratecode_id,

        nullif(trim(store_and_fwd_flag), '') as store_and_fwd_flag,

        cast(pulocationid as integer) as pu_location_id,
        cast(dolocationid as integer) as do_location_id,

        cast(payment_type as integer) as payment_type,

        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(congestion_surcharge as numeric) as congestion_surcharge,
        cast(airport_fee as numeric) as airport_fee,

        '2019-01'::text as trip_month,

        case 
            when tpep_pickup_datetime is null then 1 else 0 
        end as is_missing_pickup_ts,

        case 
            when tpep_dropoff_datetime is null then 1 else 0 
        end as is_missing_dropoff_ts,

        case
            when tpep_pickup_datetime is not null 
             and tpep_dropoff_datetime is not null
             and tpep_dropoff_datetime < tpep_pickup_datetime
            then 1 else 0
        end as is_time_inverted

    from src

)

select * from typed 