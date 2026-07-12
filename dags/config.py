import os
from dotenv import load_dotenv
load_dotenv()

PG_USER = os.environ.get("PG_USER", "root")
PG_PASS = os.environ.get("PG_PASS", "root")
PG_HOST = os.environ.get("PG_HOST", "pgdatabase")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB   = os.environ.get("PG_DB", "ny_taxi")