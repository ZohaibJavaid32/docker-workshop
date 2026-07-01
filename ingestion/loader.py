import io
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
            with cursor.copy(f"COPY {table} FROM STDIN WITH (FORMAT CSV)") as copy:
                copy.write(buffer.read())

        conn.connection.commit() 

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

            copy_chunk_to_db(df_chunk, target_table, engine)
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

        