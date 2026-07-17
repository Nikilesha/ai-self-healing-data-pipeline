# 🔄 Self-Healing E-Commerce Data Pipeline

**A production-style Apache Airflow ETL pipeline with schema drift detection, checkpoint-based recovery, quarantine logic, and automated data quality scoring — built end-to-end with Docker, PostgreSQL, and FastAPI.**

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-red?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Portfolio%20Project-yellow)

---

## Overview

This project simulates a real-world e-commerce data platform: a vendor API emits raw, messy order data, and an Airflow-orchestrated pipeline is responsible for extracting, cleaning, validating, and loading it into PostgreSQL — while surviving the kinds of failures that break naive pipelines in production: malformed rows, missing/renamed columns, database hiccups, and reruns.

It was built as a self-initiated portfolio project to demonstrate practical data engineering skills beyond tutorial-level ETL: idempotent task design, checkpoint-based recovery, data quarantine, schema drift detection, and quality scoring — the kind of resilience patterns real pipelines need when upstream data isn't clean and infrastructure isn't perfectly reliable.

**Target use cases demonstrated by this repo:**
- Ingesting paginated data from an external/vendor API with retry logic
- Isolating and quarantining bad records instead of failing the whole batch
- Detecting when the shape of incoming data changes (schema drift)
- Resuming a failed pipeline run without redoing already-completed work
- Producing a numeric data quality score and health status per run

> **Honesty note:** This README documents exactly what is implemented in the code, including partially-built features and known bugs — no aspirational claims. See [Known Issues & Implementation Gaps](#known-issues--implementation-gaps).

---

## Key Features

### 📥 Data Ingestion (`dags/tasks/extract.py`)
- Paginated GET requests against a mock vendor API (`/orders`) with a configurable page size and a hard `MAX_PAGES` safety guard
- Per-page retry logic (3 attempts, 5-second backoff) using `requests`
- Raw payload persisted to JSON, plus a separate ingestion metadata file (record count, duration, page count, status) per run
- A `processed_files` lookup intended to skip already-ingested files (**see gaps** — currently broken)

### ✅ Data Validation & Quality Scoring (`dags/tasks/validate.py`)
- Required-column enforcement, raising on missing fields
- Auto datatype correction pass (see below) before scoring
- A weighted **quality score** (0–100) computed from four dimensions: completeness (30%), uniqueness (25%), validity (30%), freshness (15%)
- Score mapped to a `health_status` of `EXCELLENT` / `GOOD` / `WARNING` / `CRITICAL`
- Optional [Great Expectations](https://greatexpectations.io/) checks (not-null, uniqueness, range) run if the library is importable
- Every run writes a timestamped JSON validation report to `data/reports/`

### 🩹 Self-Healing Engine
- **Retry logic:** Airflow-level retries (3, exponential backoff, 10-min cap) on every task, plus manual per-page retries inside `extract.py`
- **Failure isolation:** rows that fail type coercion in `transform.py` are split out into an `invalid_df` and written separately instead of failing the whole batch
- **Checkpoint recovery:** `extract`, `transform`, and `validate` each check a JSON checkpoint file before running and skip re-execution if already marked done (see gaps for `load`)
- **Quarantine:** rows/batches that fail validation (quality score < 70, or error counts exceed thresholds) are dumped to `data/quarantine/` and the task raises
- **Rollback:** on a failed database write, the in-flight batch is rolled back (`conn.rollback()`) and the attempted rows are saved to `data/rollback/` with the failure reason and stage

### 🔀 Intelligent Processing
- **Schema drift detection** (`dags/tasks/schema_drift.py`): compares incoming columns against `expected_schema.json`, logs new/missing columns, reorders columns to match the expected schema, and writes a timestamped drift report
- **Column auto-mapping** (`dags/tasks/column_mapper.py`): a static alias dictionary (e.g. `qty` → `quantity`, `email` → `customer_email`) renames known synonym columns before validation
- **Datatype auto-correction** (`dags/tasks/data_corrector.py`): coerces strings, dates, ints, and floats (stripping currency symbols/commas from numeric fields) with a per-column correction summary returned for logging

### 📊 Monitoring & Reporting
- Structured logging throughout every task (`logging` module, INFO/WARNING/ERROR levels)
- JSON reports for both schema drift and validation, timestamped per run, written to `data/reports/`
- A `generate_report` Airflow task exists at the end of the DAG as a placeholder hook (currently logs a single line — see gaps)

### 🧠 ML / Anomaly Detection
**Not implemented.** There is no model training, anomaly detection, or feature-store code in this repository, despite the git history containing an `ml_integration` branch. The "quality score" and "health status" described above are rule-based/statistical (weighted completeness/uniqueness/validity/freshness), not machine-learned. If you've seen this project described elsewhere as "AI-augmented" with anomaly detection, that description outpaces the current implementation — this README does not repeat that claim.

---

## Repository Structure

```
ai-self-healing-data-pipeline/
│
├── dags/
│   ├── ecommerce_pipeline.py       # Main Airflow DAG definition + task wrappers
│   ├── test.py                     # Minimal "hello world" DAG used to smoke-test Airflow
│   ├── tasks/
│   │   ├── extract.py              # Paginated API extraction + retry logic
│   │   ├── transform.py            # Cleaning, type coercion, failure isolation, feature engineering
│   │   ├── schema_drift.py         # Schema comparison + drift report
│   │   ├── column_mapper.py        # Static column-alias renaming
│   │   ├── data_corrector.py       # Per-column datatype correction
│   │   ├── validate.py             # Quality scoring, GE checks, quarantine logic
│   │   └── load.py                 # PostgreSQL upsert + rollback on failure
│   └── utils/
│       ├── checkpoint.py           # mark_done / load_checkpoint / clear_all
│       ├── rollback.py             # save_rollback_batch on load failure
│       └── expected_schema.json    # Canonical column list used by schema_drift.py
│
├── mock_api/
│   └── app.py                      # FastAPI vendor API serving paginated orders from CSV
│
├── data/                           # Runtime data (raw, processed, validated, quarantine,
│                                    # failed, checkpoints, rollback, reports, metadata)
│
├── docker-compose.yml              # postgres + mock api + standalone Airflow services
├── requirements.txt                # Python dependencies (Airflow, pandas, psycopg2, GE, etc.)
├── LICENSE                         # MIT
└── README.md
```

---

## Architecture

```
Vendor API (FastAPI, paginated)
        │
        ▼
   EXTRACT  ──► raw JSON + ingestion metadata
        │
        ▼
  TRANSFORM ──► type coercion, cleaning, feature engineering
        │         (invalid rows isolated → data/failed/)
        ▼
   VALIDATE ──► schema drift check → column auto-map → datatype correction
        │         → quality score → GE checks (optional)
        │         (failed batches → data/quarantine/)
        ▼
     LOAD    ──► upsert into PostgreSQL `orders` table
        │         (failed batches → rollback + data/rollback/)
        ▼
GENERATE REPORT (placeholder logging hook)
```

Each stage writes its own artifacts to disk (`data/reports`, `data/checkpoints`, etc.), so a run's history is fully inspectable after the fact — this is closer to how a real data platform is debugged than a pipeline that only logs to stdout.

---

## Pipeline Flow (Airflow DAG: `ecommerce_pipeline`)

| Task | Inputs | Outputs | Responsibilities | Failure Handling |
|---|---|---|---|---|
| `extract_data` | Vendor API (`GET /orders`, paginated) | Raw JSON (`data/raw/`) + ingestion metadata | Paginate through the API, accumulate all records, persist raw payload | 3 retries per page (5s delay) inside the task; 3 Airflow-level retries on top |
| `transform_data` | Raw JSON | Cleaned CSV (`data/processed/`) + failed rows CSV (`data/failed/`) | Column normalization, type coercion, dedup, business-rule filtering, feature engineering (`calculated_total`, `amount_mismatch`, `revenue`, etc.) | Rows failing type coercion are isolated, not dropped; any unhandled exception copies the raw file to the failed path |
| `validate_data` | Cleaned CSV | Validation report (`data/reports/`) + validated CSV (`data/validated/`) or quarantine file | Schema drift detection, column auto-mapping, datatype correction, quality scoring, optional Great Expectations checks | Batches below the quality threshold are written to `data/quarantine/` and the task raises to trigger Airflow retries |
| `load_data` | Validated CSV | Rows upserted into PostgreSQL `orders` table | Idempotent upsert (`ON CONFLICT DO UPDATE`), tracks processed files in a `processed_files` table | On DB error: transaction rollback + failed batch saved to `data/rollback/` with reason and stage |
| `generate_report` | — | Log line only | Placeholder end-of-DAG hook (`trigger_rule=ALL_DONE`, so it runs regardless of upstream failures) | N/A — no-op |

Task dependency chain: `extract_data → transform_data → validate_data → load_data → generate_report`.

---

## Self-Healing Mechanisms

| Feature | Description | Status |
|---|---|---|
| Airflow-level retries | 3 retries per task, exponential backoff, 10-min cap (`default_args`) | ✅ Implemented |
| Per-page API retry | 3 attempts per API page with fixed 5s delay | ✅ Implemented |
| Checkpoint-based skip logic | `extract`, `transform`, `validate` check a JSON checkpoint before running and skip if already done | ✅ Implemented (`load` is the exception — see gaps) |
| Failure isolation | Rows failing type coercion in `transform.py` are split out rather than failing the batch | ✅ Implemented |
| Schema drift detection | Compares actual vs. expected columns, logs and reports the diff, reorders columns | ✅ Implemented (detection + reorder only — see gaps) |
| Column auto-mapping | Static synonym dictionary renames known alias columns | ✅ Implemented |
| Datatype auto-correction | Per-column type coercion with currency/format cleanup | ✅ Implemented |
| Quarantine | Batches failing quality thresholds are written to `data/quarantine/` | ✅ Implemented |
| Database rollback | Failed load transactions are rolled back and the batch is archived | ✅ Implemented |
| Idempotent load | Upsert via `ON CONFLICT DO UPDATE`, plus a `processed_files` ledger | ✅ Implemented |
| Deduplication-on-extract | Intended to skip already-processed source files via DB lookup | ⚠️ Present but broken — see gaps |
| Missing-column auto-injection | Drift report claims missing columns are "auto-created with NULL values" | ⚠️ Logged only, not actually performed — see gaps |

---

## Tech Stack

| Category | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | DAG scheduling, retries, task dependencies |
| Language | Python 3.10 | All pipeline logic |
| Database | PostgreSQL 15 | Persisted, deduplicated order records |
| Data processing | pandas | Cleaning, transformation, feature engineering |
| API layer | FastAPI + Uvicorn | Mock vendor API serving paginated order data |
| DB driver | psycopg2 | Direct SQL execution against PostgreSQL |
| Data quality | Great Expectations (optional) | Supplementary not-null/range/uniqueness checks |
| Containerization | Docker Compose | 3-service local stack (postgres, api, airflow) |
| Config | python-dotenv | Environment variable loading |

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/Nikilesha/ai-self-healing-data-pipeline.git
cd ai-self-healing-data-pipeline

# 2. Create a .env file in the project root (see Environment Variables below)
cp .env.example .env   # create this file yourself — no example file is currently checked in

# 3. Build and start the stack (Postgres + mock API + Airflow, all via Docker)
docker compose up --build
```

There is no separate local `venv` setup path documented in this repo — `requirements.txt` is installed inside the Airflow image at build/runtime, and the mock API installs its own dependencies (`fastapi`, `uvicorn`, `pandas`) inline via its `command:` in `docker-compose.yml`.

---

## Running the Project

1. Start the stack: `docker compose up --build`
2. Airflow (standalone mode) will be available at **http://localhost:8080** — log in with `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` from your `.env`
3. The mock vendor API will be available at **http://localhost:8000/orders**
4. In the Airflow UI, unpause and trigger the `ecommerce_pipeline` DAG
5. Inspect outputs under `data/` on the host (bind-mounted from the container):
   - `data/raw/` — extracted JSON
   - `data/processed/` / `data/failed/` — transform output
   - `data/validated/` / `data/quarantine/` — validation output
   - `data/reports/` — schema drift + validation JSON reports
   - `data/rollback/` — failed load batches
   - `data/checkpoints/` — per-stage completion markers
6. Query loaded data directly in Postgres: `docker exec -it postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT * FROM orders LIMIT 10;"`

To re-run from scratch, delete the contents of `data/checkpoints/` (or call `clear_all()` from `utils/checkpoint.py`) so `extract`/`transform`/`validate` don't skip themselves.

---

## Environment Variables

Inferred from `docker-compose.yml` and `os.getenv(...)` calls across the codebase. No `.env.example` is currently committed — you'll need to create your own `.env` with these keys.

| Variable | Purpose | Example |
|---|---|---|
| `POSTGRES_USER` | Postgres username | `airflow_user` |
| `POSTGRES_PASSWORD` | Postgres password | `changeme` |
| `POSTGRES_DB` | Postgres database name | `pipeline_db` |
| `POSTGRES_HOST` | Postgres host (container name inside Compose network) | `postgres` |
| `POSTGRES_PORT` | Postgres port | `5432` |
| `AIRFLOW_USERNAME` | Airflow web UI admin username | `admin` |
| `AIRFLOW_PASSWORD` | Airflow web UI admin password | `admin` |
| `RAW_DATA_PATH` | Directory for raw extracted data | `/opt/airflow/data/raw` |
| `PROCESSED_DATA_PATH` | Directory for cleaned CSVs | `/opt/airflow/data/processed` |
| `FAILED_DATA_PATH` | Directory for isolated/failed rows | `/opt/airflow/data/failed` |
| `JSON_FILE_PATH` | Path to the raw JSON extraction target | `/opt/airflow/data/raw/orders_uncleaned.json` |
| `METADATA_PATH` | Directory for ingestion metadata files | `/opt/airflow/data/metadata` |
| `QUARANTINE_DIR_PATH` | Directory for quarantined batches | `/opt/airflow/data/quarantine` |
| `REPORT_DIR_PATH` | Directory for drift/validation reports | `/opt/airflow/data/reports` |
| `VALIDATED_DATA_PATH` | Directory for validated CSVs | `/opt/airflow/data/validated` |
| `SCHEMA_PATH` | Path to `expected_schema.json` | `/opt/airflow/dags/utils/expected_schema.json` |
| `CHECKPOINT_DIR` | Directory for stage checkpoint files | `/opt/airflow/data/checkpoints` |
| `ROLLBACK_DATA_PATH` | Directory for rolled-back load batches | `/opt/airflow/data/rollback` |
| `CSV_PATH` (mock API only) | Source CSV the mock API serves | `/opt/airflow/data/raw/orders_uncleaned.csv` |

---

## Configuration

- **`dags/utils/expected_schema.json`** — the single source of truth for the pipeline's expected column set. `schema_drift.py` diffs incoming data against this list every run and reorders columns to match it.
- **`dags/tasks/column_mapper.py`** — a hardcoded `COLUMN_MAPPING` dict of known column-name synonyms (e.g. `qty` → `quantity`). To support a new upstream naming convention, add an entry here.
- **`dags/tasks/data_corrector.py`** — an `EXPECTED_TYPES` dict controlling which columns get string/int/float/datetime coercion during validation.
- **`docker-compose.yml`** — defines the three-service stack. Airflow runs in `standalone` mode with the `SequentialExecutor` — fine for a single-machine demo, not intended for parallel/distributed execution.

---

## Logging

- All tasks use Python's standard `logging` module at INFO/WARNING/ERROR levels; Airflow captures these into its own per-task-instance logs (visible in the Airflow UI under each task run, and on disk under the `logs/` volume mount).
- Structured **JSON reports** (not just log lines) are the primary audit trail: schema drift reports and validation reports are written per-run to `data/reports/`, each timestamped.
- Ingestion metadata (record counts, duration, page counts) is written per extraction run to `data/metadata/`.
- Failures escalate through `logger.exception(...)` in the DAG wrapper functions, so full tracebacks land in Airflow's task logs.

---

## Metrics Generated

From the validation report (`data/reports/validation_report_*.json`):

| Metric | Description |
|---|---|
| `quality_score` | Weighted composite score (0–100) across completeness, uniqueness, validity, freshness |
| `health_status` | `EXCELLENT` / `GOOD` / `WARNING` / `CRITICAL`, derived from `quality_score` |
| `total_records` | Row count of the batch being validated |
| `missing_customer_name` / `missing_email` / `missing_order_date` | Null counts per field |
| `duplicate_orders` | Count of duplicate `order_id` values |
| `negative_quantity` / `negative_amount` | Business-rule violations |
| `invalid_order_id` / `invalid_quantity` / `invalid_total_amount` / `invalid_order_date` | Datatype/format violations |
| `stale_records` | Orders older than a 180-day cutoff |
| `datatype_corrections` | Per-column summary of coercions applied by `data_corrector.py` |
| `ge_results` | Optional Great Expectations pass/fail booleans, if the library is installed |

---

## Fault Recovery Workflow

| Scenario | What happens |
|---|---|
| **API request fails** | Retried up to 3 times per page with a 5s delay; if still failing, the extract task raises and Airflow retries the whole task up to 3 more times |
| **CSV/JSON corrupted or empty** | `transform.py` raises `ValueError`, the raw input file is copied to `data/failed/`, and the exception propagates to Airflow |
| **Datatype mismatch** | Handled at two layers: `transform.py` isolates rows that fail coercion into `invalid_df`; `data_corrector.py` coerces types during validation and reports what it changed |
| **Schema changes (new/missing columns)** | Detected by `schema_drift.py`, logged, reported to `data/reports/`, and columns are reordered to match the expected schema. **Missing columns are not actually injected as NULL despite the log message claiming so** — see gaps |
| **Database write fails** | Transaction is rolled back (`conn.rollback()`), the attempted batch is saved to `data/rollback/` with the failure reason and stage, and the exception is re-raised |
| **Validation fails (low quality score)** | The batch is written to `data/quarantine/` and the task raises, triggering Airflow's retry policy |
| **Task reruns after partial success** | `extract`, `transform`, and `validate` each check `data/checkpoints/<stage>.json` and skip re-execution if already marked done for that run |

---

## Screenshots

*Add screenshots here once available:*
- Airflow DAG graph view
- Sample validation report / quality score output
- Sample schema drift report
- Task logs showing a retry/self-heal in action

---

## Known Issues & Implementation Gaps

Documented honestly, as-is in the current code:

- **`extract.py` deduplication is broken:** `already_processed()` opens a DB cursor/connection, but closes both in a `finally` block *before* the `SELECT` query that follows runs — so the lookup executes against an already-closed cursor. In practice this means the "skip already-processed files" check will throw rather than work as intended.
- **Schema drift doesn't actually backfill missing columns:** `detect_schema_drift()` logs `"Missing columns auto-created with NULL values"` and records `created_missing_column:<name>` in the drift report, but no code path actually adds the column to the DataFrame (e.g. `df[col] = None`). The report is accurate about *detection*, not about the claimed remediation.
- **`load_wrapper()` has no checkpoint guard:** unlike `extract`, `transform`, and `validate`, the load stage does not check `load_checkpoint("load")` before running, so a DAG rerun will always attempt to reload data (the `processed_files` table provides a secondary, DB-level idempotency guard, but the checkpoint-skip pattern used elsewhere isn't applied here).
- **Dead code in `checkpoint.py`:** `load_checkpoint()` returns before reaching a `try/except` block intended to handle corrupted checkpoint files — that recovery path is currently unreachable.
- **No ML/anomaly detection component**, despite a git history branch named `ml_integration`. Quality scoring is deterministic/statistical, not model-based.
- **`generate_report` is a placeholder** — it logs a single line and does not aggregate metrics from the run.
- **No automated test suite.** `dags/test.py` is a minimal Airflow smoke-test DAG, not unit/integration tests for the pipeline logic.
- **No `.env.example`** is committed, so environment setup currently requires reverse-engineering variable names from `docker-compose.yml` and source (this README's table above should cover it).

---

## Future Improvements

- Fix the `extract.py` cursor lifecycle bug and add real deduplication-on-extract
- Actually inject missing columns with NULL/default values in `schema_drift.py`, matching what the report already claims
- Add a checkpoint guard to `load_wrapper()` for consistency with the other stages
- Replace `SequentialExecutor` with `LocalExecutor` (or `CeleryExecutor`) for parallel task execution
- Kafka-based ingestion for streaming order events instead of polling
- Prometheus + Grafana for pipeline health dashboards
- Formalize the Great Expectations suite instead of a small inline check set
- Automated pytest suite covering each task module
- Email/Slack alerting on quarantine or rollback events
- LLM-generated incident summaries from validation/drift reports

---

## Resume Highlights

- Designed and built a multi-stage, self-healing ETL pipeline using Apache Airflow, PostgreSQL, and FastAPI, orchestrating extract → transform → validate → load with checkpoint-based recovery.
- Implemented schema drift detection that diffs incoming data against a canonical schema and generates auditable JSON drift reports per run.
- Built a rule-based data quality scoring system (completeness, uniqueness, validity, freshness) driving automated quarantine of low-quality batches.
- Implemented idempotent database loads via PostgreSQL upserts (`ON CONFLICT DO UPDATE`) with transactional rollback and batch archival on failure.
- Containerized the full stack (Airflow, PostgreSQL, mock vendor API) with Docker Compose for one-command local deployment.
- Practiced honest engineering documentation — identified and disclosed real bugs and unfinished features in this project's own README rather than overselling scope.

---

## Skills Demonstrated

| Skill | Evidence in Project |
|---|---|
| Workflow orchestration | Airflow DAG with task dependencies, retries, and trigger rules (`dags/ecommerce_pipeline.py`) |
| Data quality engineering | Custom quality scoring model + optional Great Expectations integration (`validate.py`) |
| Resilience patterns | Checkpointing, quarantine, rollback, failure isolation across multiple task modules |
| SQL / relational design | Upsert logic, idempotency tables, transactional rollback (`load.py`) |
| API integration | Paginated REST consumption with retry/backoff (`extract.py`) |
| Containerized infrastructure | Multi-service Docker Compose stack |
| Data cleaning at scale | pandas-based type coercion, dedup, business-rule filtering, feature engineering (`transform.py`, `data_corrector.py`) |
| Engineering honesty | Documented known bugs and gaps rather than presenting the project as fully complete |

---

## Lessons Learned

Building this project surfaced several real engineering lessons:

- **Idempotency is easy to claim and hard to fully implement** — three of four pipeline stages have checkpoint guards, and even those interact with a separate DB-level idempotency table; keeping recovery logic consistent across every stage takes deliberate effort.
- **A log message is not a guarantee** — the schema drift module logs that it "auto-creates" missing columns, but the code never does; this project is a good reminder to verify claims against implementation, not just against comments/logs.
- **`try/finally` ordering matters** — closing a database cursor in a `finally` block before it's actually used (as in `extract.py`) is a small mistake with a large blast radius.
- **Failure isolation beats all-or-nothing batches** — splitting invalid rows out during transform, rather than failing the whole file, kept the pipeline usable even with dirty upstream data.

---

## License

Distributed under the [MIT License](./LICENSE).
