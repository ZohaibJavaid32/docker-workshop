import logging
import pandas as pd
from sqlalchemy import text
from ingestion.logging_config import setup_logging



logger = setup_logging()

def create_unified_table(engine) -> None:
    """ Create unified yellow_taxi table."""

    check_sql = """
        SELECT EXISTS(
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'yellow_taxi'
        );
    """
    sql = """
        CREATE TABLE IF NOT EXISTS yellow_taxi (
            "VendorID"              BIGINT,
            tpep_pickup_datetime    TIMESTAMP,
            tpep_dropoff_datetime   TIMESTAMP,
            passenger_count         DOUBLE PRECISION,
            trip_distance           DOUBLE PRECISION,
            "RatecodeID"            DOUBLE PRECISION,
            store_and_fwd_flag      TEXT,
            "PULocationID"          BIGINT,
            "DOLocationID"          BIGINT,
            payment_type            BIGINT,
            fare_amount             DOUBLE PRECISION,
            extra                   DOUBLE PRECISION,
            mta_tax                 DOUBLE PRECISION,
            tip_amount              DOUBLE PRECISION,
            tolls_amount            DOUBLE PRECISION,
            improvement_surcharge   DOUBLE PRECISION,
            total_amount            DOUBLE PRECISION,
            congestion_surcharge    DOUBLE PRECISION,
            "Airport_fee"           DOUBLE PRECISION,
            cbd_congestion_fee      DOUBLE PRECISION,
            data_year               INTEGER,
            data_month              INTEGER
        )
    """

    create_index = """
            CREATE INDEX IF NOT EXISTS idx_yello_taxi_year_month
            ON yellow_taxi(data_year , data_month)
        """
    

    with engine.connect() as conn:
        result = conn.execute(text(check_sql))
        exists = result.scalar()
        
        if not exists:
            logger.info("Creating unified yellow_taxi table")
            conn.execute(text(sql))
            conn.execute(text(create_index))
            conn.commit()
            logger.info("Unified yellow_taxi table created.")
        else:
            logger.debug("Unified yellow_taxi table ready.")