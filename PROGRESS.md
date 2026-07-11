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
- [ ] Partitioned tables by year/month

## Phase 3 — Orchestration
- [ ] Apache Airflow via Docker Compose
- [ ] Ingestion DAG (download → validate → load → notify)
- [ ] Scheduled monthly runs
- [ ] Task-level retries and alerting

## Phase 4 — Transformation Layer
- [ ] dbt project setup
- [ ] Staging models (stg_yellow_taxi)
- [ ] Intermediate + mart models
- [ ] dbt tests and documentation

## Phase 5 — Cloud & Production
- [ ] AWS S3 raw storage
- [ ] AWS RDS or managed database
- [ ] Terraform infrastructure
- [ ] GitHub Actions CI/CD
- [ ] Monitoring and alerting
