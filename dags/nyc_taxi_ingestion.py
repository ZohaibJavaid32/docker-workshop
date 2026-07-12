from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Task functions ────────────────────────────────────────────────────────────

def download_and_validate():
    print("Downloading and validating...")

def load_to_postgres():
    print("Loading to PostgreSQL...")

def create_indexes():
    print("Creating indexes...")

# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="nyc_taxi_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 1 * *",
    catchup=False,
    tags=["nyc", "taxi", "ingestion"],
) as dag:

    t1 = PythonOperator(task_id="download_and_validate", python_callable=download_and_validate)
    t2 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t3 = PythonOperator(task_id="create_indexes", python_callable=create_indexes)

    t1 >> t2 >> t3