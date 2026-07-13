WITH trips AS (
    SELECT * FROM {{ ref('int_trips_enriched')}}
),
daily AS (
    SELECT DATE(pickup_datetime) AS pickup_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue
    AVG(fare_amount) AS avg_fare
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
    SUM(tip_amount) AS total_tips
    GROUP BY DATE(pickup_datetime)
)
SELECT * FROM daily
ORDER BY pickup_date