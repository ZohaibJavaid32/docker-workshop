from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0, "/opt/airflow/dags")
from config import PG_USER, PG_PASS, PG_HOST, PG_PORT, PG_DB

YEAR       = 2025
MONTH      = 4
TARGET_TABLE = f"yellow_taxi_{YEAR}_{MONTH:02d}"
CHUNKSIZE  = 100000

# ── Task functions ────────────────────────────────────────────────────────────

def task_download_and_validate():
    import sys
    sys.path.insert(0 , "/opt/airflow")
    from ingestion.downloader import download_parquet
    from ingestion.validator import validate_dataframe

    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{YEAR}-{MONTH:02d}.parquet"
    )

    print(f"Downloading: {url}")
    df = download_parquet(url)
    print(f"Downloaded {len(df):,} rows.")
    validate_dataframe(df ,YEAR,MONTH)
    print("Validation passed.")

def task_load_to_postgres():
    import sys
    sys.path.insert(0 , "/opt/airflow")
    from ingestion.loader import load_taxi_data
    
    load_taxi_data(
        pg_user=PG_USER,
        pg_pass=PG_PASS,
        pg_host=PG_HOST,
        pg_port=PG_PORT,
        pg_db=PG_DB,
        year=YEAR,
        month=MONTH,
        target_table=TARGET_TABLE,
        chunksize=CHUNKSIZE,
    )
    print("Loading to PostgreSQL...")

# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="nyc_taxi_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule="0 6 1 * *",
    catchup=False,
    tags=["nyc", "taxi", "ingestion"],
) as dag:

    t1 = PythonOperator(task_id="download_and_validate", python_callable=task_download_and_validate)
    t2 = PythonOperator(task_id="load_to_postgres", python_callable=task_load_to_postgres)

    t1 >> t2