#!/usr/bin/env python
# coding: utf-8
import os
import click
from dotenv import load_dotenv
from ingestion.loader import load_taxi_data


# Load .env file into environment variables before anything else
load_dotenv()



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
