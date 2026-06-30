import logging
import sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from tqdm.auto import tqdm
from ingestion.logging_config import setup_logging
from ingestion.downloader import download_parquet
from ingestion.validator import validate_dataframe


# setup logging

logger = setup_logging()


DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}



def load_taxi_data(
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    year: int,
    month: int,
    target_table: str,
    chunksize: int,
) -> None:
    """Download NYC taxi Parquet file and load it into PostgreSQL."""

    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{year}-{month:02d}.parquet"
    )

    logger.info(f"Starting Ingestion for {year}-{month:02d}")
    logger.info(f"Source URL: {url}")
    logger.info(f"Target Table: {target_table}")


    engine = None
    try:

        engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        )


        logger.info(f"Downloading parquet file...")
        df = download_parquet(url)
        total_rows = len(df)
        logger.info(f"File loaded. Total rows: {total_rows:,}")

        logger.info("Performing Validations...")

        validate_dataframe(df , year , month)
        
        total_chunks = len(df) // chunksize + 1
        loaded_rows = 0
        first = True

        for start in tqdm(range(0, len(df) , chunksize) ,total=total_chunks , desc="Loading Chunks"):
            df_chunk = df.iloc[start:start + chunksize]

            if first:
                df_chunk.head(n=0).to_sql(
                    name=target_table,
                    con=engine,
                    if_exists="replace",
                    index=False
                )

                first=False

            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append",
                index=False,
                method="multi"
            )
            loaded_rows += len(df_chunk)
        
        logger.info(f"Done. Loaded {loaded_rows:,} rows into table {target_table}")
    
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        sys.exit(0)
    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
        sys.exit(1)

    except Exception as e:
        logger.exception(f"Unexpected Error: {e}")
        sys.exit(1)
    
    finally:
        if engine is not None:
            engine.dispose()
            logger.info("Database Connection Closed.")

        