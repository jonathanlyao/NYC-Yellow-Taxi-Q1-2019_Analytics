-- =========================================================
-- File: sql/exploratory_analysis.sql
-- Project: NYC Yellow Taxi Q1 2019 Analytics
-- Purpose:
--   Analytical exploration queries used to validate metrics,
--   generate business insights, and support dashboard design.
-- =========================================================


-- =========================================================
-- 1. Revenue by Pickup Borough
-- Business Question:
--   Which pickup borough contributes the most revenue?
-- =========================================================
SELECT
    pu_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY pu_borough
ORDER BY total_revenue DESC;


-- =========================================================
-- 2. Revenue by Dropoff Borough
-- Business Question:
--   Which destination borough receives the most trip revenue?
-- =========================================================
SELECT
    do_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY do_borough
ORDER BY total_revenue DESC;


-- =========================================================
-- 3. Top 10 Pickup Zones by Revenue
-- Business Question:
--   Which pickup zones generate the highest revenue?
-- =========================================================
SELECT
    pu_zone,
    pu_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY pu_zone, pu_borough
ORDER BY total_revenue DESC
LIMIT 10;


-- =========================================================
-- 4. Top 10 Dropoff Zones by Revenue
-- Business Question:
--   Which dropoff zones are associated with the highest revenue?
-- =========================================================
SELECT
    do_zone,
    do_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY do_zone, do_borough
ORDER BY total_revenue DESC
LIMIT 10;


-- =========================================================
-- 5. Revenue and Trip Volume by Payment Type
-- Business Question:
--   How do payment methods differ in revenue contribution and volume?
-- =========================================================
SELECT
    payment_type_name,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY payment_type_name
ORDER BY total_revenue DESC;


-- =========================================================
-- 6. Tip Rate by Payment Type
-- Business Question:
--   Which payment methods are associated with higher tipping behavior?
-- Note:
--   Assumes tip_amount exists in mart table.
-- =========================================================
SELECT
    payment_type_name,
    COUNT(*) AS total_trips,
    ROUND(SUM(tip_amount)::numeric, 2) AS total_tip_amount,
    ROUND(AVG(tip_amount)::numeric, 2) AS avg_tip_per_trip,
    ROUND(
        (SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric,
        2
    ) AS tip_rate_pct
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY payment_type_name
ORDER BY tip_rate_pct DESC;


-- =========================================================
-- 7. Monthly Trend: Trips and Revenue
-- Business Question:
--   How did demand and revenue evolve across Jan, Feb, and Mar?
-- =========================================================
SELECT
    trip_month,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY trip_month
ORDER BY trip_month;


-- =========================================================
-- 8. Weekday vs Weekend Performance
-- Business Question:
--   Do weekends perform differently from weekdays?
-- Note:
--   Assumes dim_date contains is_weekend.
-- =========================================================
SELECT
    d.is_weekend,
    COUNT(*) AS total_trips,
    ROUND(SUM(t.total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(t.total_amount)::numeric, 2) AS avg_revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1 t
JOIN raw_core.dim_date d
    ON t.service_date = d.full_date
GROUP BY d.is_weekend
ORDER BY d.is_weekend;


-- =========================================================
-- 9. Holiday vs Non-Holiday Performance
-- Business Question:
--   How does revenue differ between holidays and non-holidays?
-- Note:
--   Assumes dim_date contains is_holiday and holiday_type.
-- =========================================================
SELECT
    d.is_holiday,
    COUNT(*) AS total_trips,
    ROUND(SUM(t.total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(t.total_amount)::numeric, 2) AS avg_revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1 t
JOIN raw_core.dim_date d
    ON t.service_date = d.full_date
GROUP BY d.is_holiday
ORDER BY d.is_holiday;


-- =========================================================
-- 10. Revenue by Holiday Type
-- Business Question:
--   Among holiday dates, which holiday type is associated with higher revenue?
-- =========================================================
SELECT
    d.holiday_type,
    COUNT(*) AS total_trips,
    ROUND(SUM(t.total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(t.total_amount)::numeric, 2) AS avg_revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1 t
JOIN raw_core.dim_date d
    ON t.service_date = d.full_date
WHERE d.is_holiday = TRUE
GROUP BY d.holiday_type
ORDER BY total_revenue DESC;


-- =========================================================
-- 11. Average Speed by Pickup Borough
-- Business Question:
--   Which borough has the fastest / slowest average trip speed?
-- =========================================================
SELECT
    pu_borough,
    ROUND(AVG(avg_speed_mph)::numeric, 2) AS avg_speed_mph,
    COUNT(*) AS trip_count
FROM raw_mart.mart_tripdata_joined_2019_q1
WHERE avg_speed_mph IS NOT NULL
  AND avg_speed_mph BETWEEN 0.1 AND 80
GROUP BY pu_borough
ORDER BY avg_speed_mph DESC;


-- =========================================================
-- 12. Revenue Efficiency by Borough
-- Business Question:
--   Which borough generates the highest revenue per trip?
-- =========================================================
SELECT
    pu_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS revenue_per_trip,
    ROUND(AVG(trip_distance)::numeric, 2) AS avg_trip_distance,
    ROUND(AVG(avg_speed_mph)::numeric, 2) AS avg_speed_mph
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY pu_borough
ORDER BY revenue_per_trip DESC;


-- =========================================================
-- 13. Trip Duration Bucket Analysis
-- Business Question:
--   How do trip volume and revenue differ by duration segment?
-- Note:
--   Assumes trip_duration_hours exists in mart table.
-- =========================================================
SELECT
    CASE
        WHEN trip_duration_hours <= 0.25 THEN '0-15 min'
        WHEN trip_duration_hours <= 0.50 THEN '15-30 min'
        WHEN trip_duration_hours <= 1.00 THEN '30-60 min'
        WHEN trip_duration_hours <= 2.00 THEN '1-2 hrs'
        WHEN trip_duration_hours <= 5.00 THEN '2-5 hrs'
        ELSE '5+ hrs'
    END AS duration_bucket,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS revenue_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY 1
ORDER BY
    CASE
        WHEN duration_bucket = '0-15 min' THEN 1
        WHEN duration_bucket = '15-30 min' THEN 2
        WHEN duration_bucket = '30-60 min' THEN 3
        WHEN duration_bucket = '1-2 hrs' THEN 4
        WHEN duration_bucket = '2-5 hrs' THEN 5
        ELSE 6
    END;


-- =========================================================
-- 14. Congestion Surcharge by Month
-- Business Question:
--   How did congestion surcharge revenue change after the policy started?
-- =========================================================
SELECT
    trip_month,
    ROUND(SUM(congestion_surcharge)::numeric, 2) AS total_congestion_surcharge,
    ROUND(AVG(congestion_surcharge)::numeric, 2) AS avg_surcharge_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY trip_month
ORDER BY trip_month;


-- =========================================================
-- 15. Congestion Surcharge by Borough
-- Business Question:
--   Which borough was most affected by congestion surcharge?
-- =========================================================
SELECT
    pu_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(congestion_surcharge)::numeric, 2) AS total_congestion_surcharge,
    ROUND(AVG(congestion_surcharge)::numeric, 2) AS avg_surcharge_per_trip
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY pu_borough
ORDER BY total_congestion_surcharge DESC;


-- =========================================================
-- 16. Daily KPI Validation Against Daily Mart
-- Business Question:
--   Does the daily mart table reconcile with trip-level data?
-- =========================================================
SELECT
    service_date,
    total_trips_daily,
    ROUND(total_revenue_daily::numeric, 2) AS total_revenue_daily,
    ROUND(avg_revenue_per_trip_daily::numeric, 2) AS avg_revenue_per_trip_daily,
    ROUND(avg_speed_mph_weighted::numeric, 2) AS avg_speed_mph_weighted,
    ROUND(pct_clean_trips_daily::numeric * 100, 2) AS pct_clean_trips_daily_pct
FROM raw_mart.mart_daily_trip_metrics
ORDER BY service_date
LIMIT 15;


-- =========================================================
-- 17. Manhattan Share of Revenue and Trips
-- Business Question:
--   How dominant is Manhattan in the Q1 taxi business?
-- =========================================================
WITH borough_summary AS (
    SELECT
        pu_borough,
        COUNT(*) AS total_trips,
        SUM(total_amount) AS total_revenue
    FROM raw_mart.mart_tripdata_joined_2019_q1
    GROUP BY pu_borough
),
totals AS (
    SELECT
        SUM(total_trips) AS grand_total_trips,
        SUM(total_revenue) AS grand_total_revenue
    FROM borough_summary
)
SELECT
    b.pu_borough,
    b.total_trips,
    ROUND(b.total_revenue::numeric, 2) AS total_revenue,
    ROUND((b.total_trips * 100.0 / t.grand_total_trips)::numeric, 2) AS trip_share_pct,
    ROUND((b.total_revenue * 100.0 / t.grand_total_revenue)::numeric, 2) AS revenue_share_pct
FROM borough_summary b
CROSS JOIN totals t
ORDER BY revenue_share_pct DESC;


-- =========================================================
-- 18. Daily Revenue Trend for Chart Validation
-- Business Question:
--   What does daily revenue look like across the quarter?
-- =========================================================
SELECT
    service_date,
    total_revenue_daily
FROM raw_mart.mart_daily_trip_metrics
ORDER BY service_date;


-- =========================================================
-- 19. Clean Trip Rate Validation
-- Business Question:
--   What percentage of trips are classified as clean?
-- =========================================================
SELECT
    COUNT(*) AS total_trips,
    SUM(CASE WHEN avg_speed_mph BETWEEN 0.1 AND 80 THEN 1 ELSE 0 END) AS clean_trips,
    ROUND(
        (
            SUM(CASE WHEN avg_speed_mph BETWEEN 0.1 AND 80 THEN 1 ELSE 0 END) * 100.0
            / COUNT(*)
        )::numeric,
        2
    ) AS clean_trip_rate_pct
FROM raw_mart.mart_tripdata_joined_2019_q1;


-- =========================================================
-- 20. Outlier Review
-- Business Question:
--   How many trips fall outside the expected speed range?
-- =========================================================
SELECT
    CASE
        WHEN avg_speed_mph < 0.1 THEN 'Too Slow'
        WHEN avg_speed_mph > 80 THEN 'Too Fast'
        ELSE 'Clean'
    END AS speed_quality_bucket,
    COUNT(*) AS trip_count
FROM raw_mart.mart_tripdata_joined_2019_q1
GROUP BY 1
ORDER BY trip_count DESC;