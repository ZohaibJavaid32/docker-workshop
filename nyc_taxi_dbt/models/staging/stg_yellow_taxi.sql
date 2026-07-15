WITH source AS (
    SELECT * FROM yellow_taxi
),
renamed AS (
    SELECT 
        "VendorID" AS vendor_id,
        "tpep_pickup_datetime" AS pickup_datetime,
        "tpep_dropoff_datetime" AS dropoff_datetime,
        "passenger_count" AS passenger_count,
        "trip_distance" AS trip_distance,
        "PULocationID" AS pickup_location_id,
        "DOLocationID" AS dropoff_location_id,
        "payment_type" AS payment_type,
        "fare_amount" AS fare_amount,
        "tip_amount" AS tip_amount,
        "total_amount" AS total_amount,
        "congestion_surcharge" AS congestion_surcharge
    FROM source 
    WHERE fare_amount > 0 
    AND trip_distance > 0
)
SELECT * FROM renamed
