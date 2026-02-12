import pandas as pd
from sqlalchemy import create_engine
import click 

def run(
        pg_user:str = "root",
        pg_pass:str = "root",
        pg_host:str = "localhost",
        pg_db:str = "ny_taxt",
        pg_port:int = 5432,
        target_table:str = " zones"
):

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    df = pd.read_csv(url)

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False
    )

    print("Zones Loaded Successfully.")

@click.command()
@click.option("--pg-user", default="root", show_default=True)
@click.option("--pg-pass", default="root", show_default=True)
@click.option("--pg-host", default="localhost", show_default=True)
@click.option("--pg-port", default=5432, type=int, show_default=True)
@click.option("--pg-db", default="ny_taxi", show_default=True)
@click.option("--target-table", default="zones", show_default=True)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    run(
        pg_user=pg_user,
        pg_pass=pg_pass,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_db=pg_db,
        target_table=target_table,
    )


if __name__ == "__main__":
    main()


