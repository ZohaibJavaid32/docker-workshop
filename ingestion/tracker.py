from sqlalchemy import text
from ingestion.logging_config import setup_logging

logger = setup_logging()

def create_tracking_table(engine) -> None:
    """Create ingestion_log table if its not created."""

    # Check if table already exists.
    check_sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'ingestion_log'
            );
        """
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

    
    with engine.begin() as conn:
        result = conn.execute(text(check_sql))
        exists = result.scalar()

        if not exists:
            logger.info("Creating table ingestion_log.....")
            conn.execute(text(sql))
            logger.info("Ingestion table created.")
        else:
            logger.debug("Ingestion table already exists.")
    

def is_already_loaded(engine, year: int, month: int) -> bool:

    """Check if data is loaded for this table."""

    sql = text("""
        SELECT EXISTS(
            SELECT 1 FROM yellow_taxi 
            WHERE data_year = :year
            AND data_month = :month
            LIMIT 1
        )
    """)

    logger.info(f"Checking if data exists in yellow_taxi for {year}-{month:02d}...")
    with engine.begin() as conn:
        result = conn.execute(sql,{"year": year,"month": month})
        exists = result.scalar()
        return exists

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
        