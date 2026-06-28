import logging
from pathlib import Path
import sys


# ── Logging Setup ────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    """Configure  logging to write log message to both terminal and log file."""

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_format = "[%(asctime)s] %(levelname)-8s %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers = [
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "ingest.log"),
        ],
    )

    return logging.getLogger(__name__)
