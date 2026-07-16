from sqlalchemy import text
from ingestion.logging_config import setup_logging

logger = setup_logging()

def create_tracking_table(engine) -> None:
    """Create ingestion_log table if its not created."""
    sql = """
        CREATE TABLE IF NOT EXISTS ingestion_log (
            id SERIAL PRIMARY KEY,
            table_name TEXT,
            year INTEGER,
            month INTEGER,
            rows_loaded BIGINT,
            loaded_at TIMESTAMP DEFAULT NOW(),
            status TEXT
        )
    """

    logger.info("Creating table ingestion_log.....")
    with engine.begin() as conn:
        conn.execute(text(sql))
    
    logger.info("ingestion_log table created.")

def is_already_loaded(engine , table : str, year: int, month: int) -> bool:

    """Check if data is loaded for this table."""

    sql = text("""
        SELECT COUNT(*)
        FROM ingestion_log
        WHERE table_name = :table
        AND year = :year
        AND month = :month
        AND LOWER(status) = 'success'
    """)

    logger.info(f"Checking success status for '{table}'.")
    with engine.connect() as conn:
        count = conn.execute(
            sql,
            {
                "table": table,
                "year": year,
                "month": month,
            },
        ).scalar()  

    return count > 0

def log_ingestion(engine , table :str , year : int , month : int , rows_loaded: int , status : str) -> None:
    """Log metrics for loaded table."""

    sql = text("""
        INSERT INTO ingestion_log
        (table_name, year, month, rows_loaded, status)
        VALUES 
        (:table , :year , :month , :rows_loaded , :status)
    """)

    logger.info(f"Inserting metrics for '{table}'.")
    with engine.begin() as conn:
        conn.execute(
            sql,
            {"table":table,
             "year":year,
             "month":month,
             "rows_loaded": rows_loaded,
             "status": status.lower()
            },
        )
        