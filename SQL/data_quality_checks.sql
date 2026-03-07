-- abnormal timestamps
SELECT *
FROM raw_core.fact_yellow_tripdata_2019_q1
WHERE pickup_datetime < '2019-01-01';

-- negative duration
SELECT *
FROM raw_core.fact_yellow_tripdata_2019_q1
WHERE dropoff_datetime <= pickup_datetime;

-- extreme speed
SELECT *
FROM raw_core.fact_yellow_tripdata_2019_q1
WHERE avg_speed_mph > 80;