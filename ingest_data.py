#!/usr/bin/env python
# coding: utf-8

import os
import sys

import click
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from tqdm.auto import tqdm

# Load .env file into environment variables before anything else
load_dotenv()

# ── Schema definition ────────────────────────────────────────────────────────

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

# ── Core ingestion function ───────────────────────────────────────────────────

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

    print(f"Downloading: {url}")

    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    try:
        df = pd.read_parquet(url)
        print(f"File loaded. Total rows: {len(df):,}")

        total_chunks = (len(df) // chunksize) + 1
        first = True

        for start in tqdm(range(0, len(df), chunksize), total=total_chunks, desc="Loading chunks"):
            df_chunk = df.iloc[start: start + chunksize]

            if first:
                df_chunk.head(n=0).to_sql(
                    name=target_table,
                    con=engine,
                    if_exists="replace",
                    index=False,
                )
                first = False

            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
            )

        print(f"\nDone. All data loaded into table: {target_table}")

    except KeyboardInterrupt:
        print("\nInterrupted by user. Cleaning up...")
        sys.exit(0)

    except SQLAlchemyError as e:
        print(f"\nDatabase error: {e}")
        sys.exit(1)

    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)

    finally:
        engine.dispose()
        print("Database connection closed.")


# ── CLI definition ────────────────────────────────────────────────────────────

@click.command()
@click.option("--pg-user",      default=lambda: os.environ.get("PG_USER", "root"),               show_default="from env", help="Postgres user")
@click.option("--pg-pass",      default=lambda: os.environ.get("PG_PASS", "root"),               show_default="from env", help="Postgres password")
@click.option("--pg-host",      default=lambda: os.environ.get("PG_HOST", "localhost"),          show_default="from env", help="Postgres host")
@click.option("--pg-port",      default=lambda: int(os.environ.get("PG_PORT", "5432")),          show_default="from env", help="Postgres port",    type=int)
@click.option("--pg-db",        default=lambda: os.environ.get("PG_DB",   "ny_taxi"),            show_default="from env", help="Postgres database")
@click.option("--year",         default=lambda: int(os.environ.get("YEAR",  "2025")),            show_default="from env", help="Year of dataset",  type=int)
@click.option("--month",        default=lambda: int(os.environ.get("MONTH", "1")),               show_default="from env", help="Month of dataset", type=int)
@click.option("--target-table", default=lambda: os.environ.get("TARGET_TABLE", "yellow_taxi_data"), show_default="from env", help="Target table name")
@click.option("--chunksize",    default=lambda: int(os.environ.get("CHUNKSIZE", "100000")),      show_default="from env", help="Chunk size",       type=int)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """NYC Taxi data ingestion pipeline."""
    load_taxi_data(
        pg_user=pg_user,
        pg_pass=pg_pass,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_db=pg_db,
        year=year,
        month=month,
        target_table=target_table,
        chunksize=chunksize,
    )


if __name__ == "__main__":
    main()
