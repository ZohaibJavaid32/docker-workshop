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
from ingestion.partitions import create_month_partition
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


def copy_chunk_to_db(df_chunk: pd.DataFrame , table :str , conn) -> None:

    # write chunk to an in memory CSV buffer (no disk writes) 
    buffer = io.StringIO()
    df_chunk.to_csv(buffer, index=False ,header=False)

    # reset buffer to start so that PostgreSQL reads from beginning
    buffer.seek(0)


        # get raw psycopg cursor from the connection
    with conn.connection.cursor() as cursor:

            # stream CSV buffer directly into PostgreSQL
        cursor.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV)",buffer)


def create_indexes(engine, table : str) -> None:

    indexes = [
        {
            "name": f"idx_{table}_pickup_datetime",
            "sql": f"CREATE INDEX IF NOT EXISTS idx_{table}_pickup_datetime ON {table} (tpep_pickup_datetime)",
        },
        {
            "name": f"idx_{table}_pu_location",
            "sql": f'CREATE INDEX IF NOT EXISTS idx_{table}_pu_location ON {table} ("PULocationID")',
        },
        {
            "name": f"idx_{table}_do_location",
            "sql": f'CREATE INDEX IF NOT EXISTS idx_{table}_do_location ON {table} ("DOLocationID")',
        },
        {
            "name": f"idx_{table}_payment_type",
            "sql": f"CREATE INDEX IF NOT EXISTS idx_{table}_payment_type ON {table} (payment_type)",
        },
    ]


    # begin() auto-commits on clean exit and auto-rolls-back on exception
    with engine.begin() as conn:

        for index in indexes:
            check_sql = f"""
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE tablename = '{table}'
                    AND indexname = '{index["name"]}'
                );
            """
            result = conn.execute(text(check_sql))
            exists = result.scalar()

            if not exists:
                logger.info(f"Creating index {index['name']}.")
                conn.execute(text(index["sql"]))
                logger.info(f"Index {index['name']} created.")
            else:
                logger.debug(f"Index {index['name']} already exists.")
            

  

def load_taxi_data(
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    year: int,
    month: int,
    chunksize: int,
) -> None:
    """Download NYC taxi Parquet file and load it into PostgreSQL."""

    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{year}-{month:02d}.parquet"
    )

    logger.info(f"Starting Ingestion for {year}-{month:02d}")
    logger.info(f"Source URL: {url}")


    engine = None
    try:

        engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}",
        pool_pre_ping=True
        )

        create_tracking_table(engine)
        create_unified_table(engine)
        create_month_partition(engine , year , month)

        if is_already_loaded(engine, year, month):
            logger.warning(f"Data for {year}-{month:02d} already loaded. Skipping..." )
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

        try:
            with engine.begin() as conn:
                for start in tqdm(range(0, len(df) , chunksize) ,total=total_chunks , desc="Loading Chunks"):
                    df_chunk = df.iloc[start:start + chunksize].copy()
                    df_chunk["data_year"] = year
                    df_chunk["data_month"] = month
                    
                    
                    copy_chunk_to_db(df_chunk, "yellow_taxi", conn=conn)
                    loaded_rows += len(df_chunk)
        except SQLAlchemyError as e:
            logger.error(f"Load failed for {year}-{month:02d}. Rolled backed cleanly: {e}")
            log_ingestion(engine , "yellow_taxi" , year , month , loaded_rows , "failed")
            raise

        create_indexes(engine , "yellow_taxi")
        logger.info(f"Done. Loaded {loaded_rows:,} rows.")

        log_ingestion(engine , "yellow_taxi" , year , month , loaded_rows , "success")

    
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
        raise

    except Exception as e:
        logger.exception(f"Unexpected Error: {e}")
        raise 
    
    finally:
        if engine is not None:
            engine.dispose()
            logger.info("Database Connection Closed.")

        