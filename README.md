# NYC Taxi Data Pipeline

A production-style data engineering pipeline that downloads NYC Yellow Taxi trip data, validates it, and loads it into PostgreSQL running inside Docker.

Built incrementally as a learning project covering Docker, PostgreSQL, Python, SQLAlchemy, and data engineering best practices.

---

## Architecture
NYC TLC CloudFront API
(Parquet files via HTTPS)
↓
Python Ingestion Script
(ingest_data.py — Click CLI)
↓
download_parquet()
(tenacity retry logic)
↓
validate_dataframe()
(row count, schema, nulls, date range)
↓
load_taxi_data()
(chunked loading via SQLAlchemy)
↓
PostgreSQL Container (pgdatabase)
↓
pgAdmin Container (browser UI)
All containers connected via Docker Compose network
---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.13 | Core language |
| Pandas + PyArrow | Data processing and Parquet reading |
| SQLAlchemy | Database abstraction layer |
| psycopg3 | PostgreSQL driver |
| Click | CLI interface |
| tqdm | Progress bars |
| tenacity | Retry logic with exponential backoff |
| python-dotenv | Environment variable management |
| PostgreSQL 18 | Target database |
| pgAdmin 4 | Database browser UI |
| Docker + Docker Compose | Containerization and networking |
| uv | Python package management |

---

## Project Structure
pipeline/
├── ingestion/
│   ├── init.py
│   ├── logging_config.py   # Logging setup (terminal + file)
│   ├── downloader.py        # download_parquet() with retry
│   ├── validator.py          # validate_dataframe() checks
│   └── loader.py              # load_taxi_data() orchestrator
├── ingest_data.py            # CLI entry point (Click)
├── docker-compose.yaml       # PostgreSQL + pgAdmin services
├── Dockerfile                # Ingestion container
├── pyproject.toml            # Dependencies
└── .env                      # Credentials (not committed)
---

## Setup

### Prerequisites
- Docker and Docker Compose
- Python 3.13+
- uv package manager

### 1. Clone the repository
```bash
git clone https://github.com/ZohaibJavaid32/docker-workshop.git
cd docker-workshop/pipeline
```

### 2. Create your `.env` file
```bash
cp .env.example .env
```
Edit `.env` with your credentials:
PG_USER=root
PG_PASS=root
PG_HOST=localhost
PG_PORT=5432
PG_DB=ny_taxi
YEAR=2025
MONTH=1
TARGET_TABLE=yellow_taxi_2025_01
CHUNKSIZE=100000
### 3. Install dependencies
```bash
uv sync
source .venv/bin/activate
```

### 4. Start PostgreSQL and pgAdmin
```bash
docker compose up -d
```

### 5. Run the pipeline
```bash
python ingest_data.py
```

---

## Usage

```bash
# Run with default values from .env
python ingest_data.py

# Override specific options
python ingest_data.py --year 2025 --month 3 --target-table yellow_taxi_2025_03

# See all options
python ingest_data.py --help
```

### CLI Options

| Option | Default | Description |
|---|---|---|
| `--pg-user` | from env | PostgreSQL username |
| `--pg-pass` | from env | PostgreSQL password |
| `--pg-host` | from env | PostgreSQL host |
| `--pg-port` | from env | PostgreSQL port |
| `--pg-db` | from env | PostgreSQL database name |
| `--year` | from env | Year of dataset |
| `--month` | from env | Month of dataset |
| `--target-table` | from env | Target table name |
| `--chunksize` | from env | Rows per chunk |

---

## Data Source

NYC TLC Yellow Taxi Trip Records from the official CloudFront endpoint:
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
Available years: 2022 onwards.

---

## Data Validation

Before loading, every file is validated for:
- Row count greater than zero
- Required columns present
- Critical columns under 50% null
- Date range matches requested year and month

---

## Logs

All pipeline activity is logged to both terminal and `logs/ingest.log`:
[2026-07-01 06:41:00] INFO     Starting ingestion for 2025-01
[2026-07-01 06:41:00] INFO     File loaded. Total rows: 3,475,226
[2026-07-01 06:41:00] INFO     Validation passed.
[2026-07-01 06:45:20] INFO     Done. Loaded 3,475,226 rows into table test_run
---

## Roadmap

- [x] Phase 1 — Hardened foundation (logging, retries, validation, modular design)
- [ ] Phase 2 — Performance (PostgreSQL COPY, indexes, incremental loading)
- [ ] Phase 3 — Orchestration (Apache Airflow)
- [ ] Phase 4 — Transformations (dbt)
- [ ] Phase 5 — Cloud and CI/CD (AWS, Terraform, GitHub Actions)
