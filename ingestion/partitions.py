import logging 
from sqlalchemy import text
from ingestion.logging_config import setup_logging

logger = setup_logging()

def create_month_partition(engine , year : int, month: int) -> None:
    
    """Create a monthly partition for yellow_taxi if it doesn't exist."""

    partition_name = f"yellow_taxi_y{year}m{month:02d}"

    # Calculate next month for upper bound
    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month + 1
    
    sql = f"""
        CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF yellow_taxi
        FOR VALUES ({year} , {month}) TO ({next_year} , {next_month})
    """

    logger.info(f"Ensuring partition {partition_name} exists...")
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info(f"Partition {partition_name} ready.")