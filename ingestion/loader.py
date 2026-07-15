import io
import logging
import sys
import pandas as pd
from sqlalchemy import create_engine , text
from sqlalchemy.exc import SQLAlchemyError
from tqdm.auto import tqdm
from ingestion.logging_config import setup_logging
from ingestion.downloader import download_parquet
from ingestion.validator import validate_dataframe
from ingestion.tracker import create_tracking_table, is_already_loaded, log_ingestion
from ingestion.unified_loader import create_unified_table

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


def copy_chunk_to_db(df_chunk: pd.DataFrame , table :str , engine) -> None:

    # write chunk to an in memory CSV buffer (no disk writes) 
    buffer = io.StringIO()
    df_chunk.to_csv(buffer, index=False ,header=False)

    # reset buffer to start so that PostgreSQL reads from beginning
    buffer.seek(0)

    # get raw conn from SQLAlchemy engine pool
    with engine.connect() as conn:

        # get raw psycopg cursor from the connection
        with conn.connection.cursor() as cursor:

            # stream CSV buffer directly into PostgreSQL
            cursor.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV)",buffer)

        conn.connection.commit() 

def create_indexes(engine, table : str) -> None:

    indexes = [
        f'CREATE INDEX IF NOT EXISTS idx_{table}_pickup_datetime ON {table} ("tpep_pickup_datetime")',
        f'CREATE INDEX IF NOT EXISTS idx_{table}_pu_location ON {table} ("PULocationID")',
        f'CREATE INDEX IF NOT EXISTS idx_{table}_do_location ON {table} ("DOLocationID")',
        f'CREATE INDEX IF NOT EXISTS idx_{table}_payment_type ON {table} ("payment_type")'
    ]

    logger.info(f"Creating indexes on {table}...")

    with engine.connect() as conn:
        for sql in indexes:
            conn.execute(text(sql))
        

    logger.info("Indexes created.")
    

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
        f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        )

        create_tracking_table(engine)

        create_unified_table(engine)

        if is_already_loaded(engine , target_table ,year , month):
            logger.warning(f"{target_table} for {year}-{month:02d} already loaded. Skipping..." )
            return
        
        logger.info(f"Downloading parquet file...")
        df = download_parquet(url)
        total_rows = len(df)
        logger.info(f"File loaded. Total rows: {total_rows:,}")

        logger.info("Performing Validations...")

        validate_dataframe(df , year , month)
        
        total_chunks = len(df) // chunksize + 1
        loaded_rows = 0
        #first = True

        for start in tqdm(range(0, len(df) , chunksize) ,total=total_chunks , desc="Loading Chunks"):
            df_chunk = df.iloc[start:start + chunksize].copy()
            df_chunk["data_year"] = year
            df_chunk["data_month"] = month
            
            
            copy_chunk_to_db(df_chunk, "yellow_taxi", engine)
            loaded_rows += len(df_chunk)
        create_indexes(engine , "yellow_taxi")
        logger.info(f"Done. Loaded {loaded_rows:,} rows into table {target_table}")

        log_ingestion(engine , target_table , year , month , loaded_rows , "success")

    
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

        