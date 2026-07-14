WITH trips AS (
    SELECT * FROM {{ ref('int_trips_enriched')}}
),
transformed AS (
    SELECT pickup_location_id,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes
    FROM trips
    GROUP BY pickup_location_id
)
SELECT * FROM transformed
ORDER BY total_trips DESC