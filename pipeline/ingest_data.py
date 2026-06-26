#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import  SQLAlchemyError
from tqdm.auto import tqdm
import pyarrow.parquet as pq
import sys
import click




dtype = {
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
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]





def load_taxi_data(
    pg_user: str = 'root',
    pg_pass: str = 'root',
    pg_host: str = 'localhost',
    pg_port: int = 5432,
    pg_db: str = 'ny_taxi',
    year: int = 2021,
    month: int = 1,
    target_table: str = 'yellow_taxi_data',
    chunksize: int = 10000,
):

    #prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    #url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_parquet(url)

    try:
        first=True
        for start in tqdm(range(0, len(df), chunksize)):
            df_chunk = df.iloc[start:start + chunksize]
                
            if first:
                df_chunk.head(n=0).to_sql(
                    name=target_table ,
                    con=engine, 
                    if_exists='replace',
                    index=False)
                    
                first=False
                
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists='append',
                index=False
            )
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting... (Ctrl+C)")
        sys.exit(0)
    except SQLAlchemyError as e:
        print(f"\nDatabase Error Occurred : {e}")
    except Exception as e:
        print(f"\nUnexpected error occurred: {e}")
        sys.exit(1)
    finally:
        engine.dispose()
        print("Database connection closed.")



if __name__ == '__main__':
    # keep default behavior when invoked without args
    # but provide a click CLI entrypoint below
    @click.command()
    @click.option('--pg-user', default='root', show_default=True, help='Postgres user')
    @click.option('--pg-pass', default='root', show_default=True, help='Postgres password')
    @click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
    @click.option('--pg-port', default=5432, type=int, show_default=True, help='Postgres port')
    @click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database')
    @click.option('--year', default=2021, type=int, show_default=True, help='Year of dataset')
    @click.option('--month', default=1, type=int, show_default=True, help='Month of dataset')
    @click.option('--target-table', default='yellow_taxi_data', show_default=True, help='Target table name')
    @click.option('--chunksize', default=100000, type=int, show_default=True, help='CSV read chunksize')

    def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
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

    main()
