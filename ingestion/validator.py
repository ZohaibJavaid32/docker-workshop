import logging
import pandas as pd
from ingestion.logging_config import setup_logging

logger = setup_logging()

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
]

def validate_dataframe(df: pd.DataFrame , year:int , month: int):
    """Validate DataFrame before loading into PostgresSQL."""

    logger.info("Running data validation...")

    if len(df) == 0:
        raise ValueError("Validation failed: DataFrame is Empty.")
    logger.info(f"Row count check passed: {len(df):,} rows found.")

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Validation failed: missing columns {missing}")
    logger.info("Schema check passed: all required columns present.")

    for col in ["tpep_pickup_datetime", "fare_amount"]:
        null_pct = df[col].isnull().mean()

        if null_pct > 0.5:
            raise ValueError(
                f"Validation failed: col {col} is {null_pct:.1%} null — exceeds 50% threshold."
            )

   logger.info("Null check passed: critical columns within acceptable threshold.")


    in_range = (
        (df["tpep_pickup_datetime"].dt.year == year) &
        (df["tpep_pickup_datetime"].dt.month == month)
    )
    if not in_range.any():
        raise ValueError(
            f"Validation failed: No rows found for {year}-{month:02d}."
        )
    logger.info(f"Date range check passed: data contains rows for {year}-{month:02d}.")

    logger.info("Validation passed.")
