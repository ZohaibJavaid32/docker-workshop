import logging
from ingestion.logging_config import setup_logging
import pandas as pd
from tenacity import retry , stop_after_attempt , wait_exponential , retry_if_exception_type, before_sleep_log

logger = setup_logging()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1,min=2,max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def download_parquet(url: str) -> pd.DataFrame:
    """Download a Parquet File from a URL with automatic retry on Failure."""
    logger.info(f"Attempting to download: {url}")
    return pd.read_parquet(url)

