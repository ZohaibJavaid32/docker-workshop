# NYC Taxi Pipeline — Progress Tracker

## Phase 1 — Harden the Foundation
- [x] `.env` + python-dotenv
- [x] `logging` module
- [x] `tenacity` retries
- [x] Refactor into `ingestion/` package
- [x] `loader.py` — complete the loader module
- [x] Data validation — wire validator into loader
- [x] `ingest_data.py` — update CLI to use new package
- [x] End-to-end test — run full pipeline locally
- [x] `README.md` — architecture, setup, usage

## Phase 2 — Performance & Scalability
- [x] PostgreSQL COPY command (replace to_sql)
- [x] Database indexes on datetime and location columns
- [x] Incremental loading (skip already-loaded months)
- [x] Partitioned tables by year/month

## Phase 3 — Orchestration
- [x] Apache Airflow via Docker Compose
- [x] Ingestion DAG (download → validate → load → notify)
- [x] Scheduled monthly runs
- [x] Task-level retries and alerting

## Phase 4 — Transformation Layer
- [x] dbt project setup
- [x] Staging models (stg_yellow_taxi)
- [x] Intermediate + mart models
- [x] dbt tests and documentation

## Phase 5 — Cloud & Production
- [ ] AWS S3 raw storage
- [ ] AWS RDS or managed database
- [ ] Terraform infrastructure
- [ ] GitHub Actions CI/CD
- [ ] Monitoring and alerting
