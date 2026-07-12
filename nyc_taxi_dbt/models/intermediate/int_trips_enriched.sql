WITH staged AS (
        SELECT * FROM {{ref('stg_yellow_taxi')}}
),
enriched AS(
    SELECT 
        *,
        EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60 AS trip_duration_minutes,
        CASE WHEN trip_distance > 0 THEN total_amount / trip_distance ELSE 0 END AS revenue_per_mile,
        tip_amount > 0 AS tipped_amount,
        EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
        EXTRACT(DOW FROM pickup_datetime) AS pickup_day_of_week
    FROM staged
)
SELECT * FROM enriched