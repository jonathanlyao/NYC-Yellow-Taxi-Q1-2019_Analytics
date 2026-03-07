{{ config(materialized='view') }}

with unioned as (

    select
        "VendorID" as vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        "RatecodeID" as ratecodeid,
        store_and_fwd_flag,
        "PULocationID" as pulocationid,
        "DOLocationID" as dolocationid,
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
    from {{ source('raw','yellow_tripdata_2019_01') }}

    union all

    select
        "VendorID" as vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        "RatecodeID" as ratecodeid,
        store_and_fwd_flag,
        "PULocationID" as pulocationid,
        "DOLocationID" as dolocationid,
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
    from {{ source('raw','yellow_tripdata_2019_02') }}

    union all

    select
        "VendorID" as vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        "RatecodeID" as ratecodeid,
        store_and_fwd_flag,
        "PULocationID" as pulocationid,
        "DOLocationID" as dolocationid,
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
    from {{ source('raw','yellow_tripdata_2019_03') }}

),

typed as (

    select
        cast(vendorid as integer) as vendor_id,
        cast(tpep_pickup_datetime as timestamp) as pickup_ts,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,

        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,

        cast(ratecodeid as integer) as rate_code_id,
        cast(store_and_fwd_flag as text) as store_and_fwd_flag,

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
        cast(airport_fee as numeric) as airport_fee
    from unioned
),

filtered as (

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
    from typed
    where pickup_ts >= timestamp '2019-01-01'
      and pickup_ts <  timestamp '2019-04-01'
      and pickup_ts is not null
      and dropoff_ts is not null
      and dropoff_ts >= pickup_ts

)

select * from filtered