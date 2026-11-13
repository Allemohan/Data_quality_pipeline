# Submission Document — Data Quality, Quarantine & Controlled Replay Pipeline

> **Author**: Mohan Venkateswara Rao Alle · `allemohanvenkateswararao@gmail.com`  
> **Date**: 13 November 2026  
> **Repository**: _see GitHub link after push_

---

## 01 · MAKE IT RUN — Quick-start (< 5 minutes, zero external services)

### Prerequisites
| Tool | Min Version | Check |
|---|---|---|
| Python | 3.11+ | `python --version` |
| pip | 23+ | `pip --version` |
| Docker + Compose | optional | `docker compose version` |

### Steps

```bash
# 1. Clone
git clone https://github.com/<YOUR_GITHUB_USERNAME>/data-quality-pipeline.git
cd data-quality-pipeline

# 2. Create & activate virtual environment
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment (SQLite fallback — no Docker needed)
cp .env.example .env
# Default .env already sets DATABASE_URL=sqlite:///./data_pipeline.db

# 5. Generate synthetic dataset (1,000 rows, seed=42, deterministic)
python -m generators.generate_data --rows 1000 --seed 42

# 6. Run ingestion pipeline
python -m src.main ingest --input data/input/customers.csv

# 7. Run automated test suite
pytest

# 8. Run with coverage report
pytest --cov=src --cov-report=term-missing
```

### Verify Key Behaviour

| Command | Expected Output |
|---|---|
| `python -m src.main ingest --input data/input/customers.csv` | `RECONCILIATION: 1000 = 920 + 60 + 20 → PASS` |
| `pytest` | `23 passed` |
| `pytest --cov=src` | `91% overall coverage` |
| `python -m src.main reconcile` | `Orphan Referential Keys: 0 (PASS)` |
| `python -m src.main replay --limit 500` | `Eligible Records Found: 0` (no CORRECTED records yet) |
| Run ingest twice on same file | `Target Updated: 920, Inserted: 0` — idempotency confirmed |

### Optional: PostgreSQL via Docker

```bash
docker compose up -d
# Update .env:
# DATABASE_URL=postgresql+psycopg://pipeline_user:pipeline_pass@localhost:5433/pipeline_db
python -m src.main ingest --input data/input/customers.csv
```

### Optional: Performance Benchmark (100 K records, ~38 s)

```bash
python -m benchmarks.run_benchmark
```

---

## 02 · SHOW YOUR WORK — Commit History

The repository contains two meaningful, self-describing commits:

| SHA | Message | What it does |
|---|---|---|
| `49068e1` | `feat: complete data quality, quarantine, and controlled replay pipeline` | Entire codebase: 50 files, 6,082 lines. Schema, validators, ingestion, quarantine, replay, CLI, tests, benchmark, generator. |
| `2574b67` | `chore: normalize line endings` | CRLF → LF normalization for cross-platform reproducibility (Windows dev → Linux CI). |

The single large commit is intentional: this was a **time-boxed build**, built top-to-bottom from scratch in one focused session. Code evolution is documented through **function-level docstrings**, **typed interfaces**, and the **test suite** (each test file represents one architectural layer).

---

## 03 · USE LLMs TRANSPARENTLY — AI Disclosure

### Tools Used
- **Antigravity (Gemini / Claude)** — primary agentic assistant

### Prompt History Summary

| Phase | Prompt Intent | AI Role | My Role |
|---|---|---|---|
| **Architecture Design** | "Design a production-style data quality pipeline with quarantine and replay for CSV → PostgreSQL" | Generated architecture diagram, proposed 7-layer structure, suggested Pydantic v2 for validation | Reviewed architecture, chose SQLite fallback, reduced scope from Kafka/Airflow to pure Python for local runnability |
| **Schema & Models** | "Generate SQLAlchemy 2.0 ORM models for customers, quarantine_records, pipeline_runs, replay_runs" | Scaffolded ORM models with relationships | Added `source_row_number`, `run_id` lineage fields myself; validated FK constraints match SQL DDL |
| **Validation Engine** | "Write Pydantic validators for email regex, status enum, future date, referential integrity" | Generated initial validator code | Rewrote duplicate_validator to use `seen_ids` set (AI used a dict unnecessarily); added boundary edge cases |
| **Quarantine State Machine** | "Implement QUARANTINED → CORRECTED → REPLAYED / REPLAY_FAILED lifecycle" | Generated state transition code | Added `retry_count` increment on `REPLAY_FAILED`; AI initially reset it |
| **Test Suite** | "Write pytest tests for each layer using in-memory SQLite fixtures" | Generated test scaffolds | Personally validated all fixture isolation, added boundary tests (empty file, header-only, negative IDs) |
| **README / Docs** | "Write comprehensive README with mermaid diagrams, data dictionary, error code table" | Generated documentation structure | Reviewed every code block for accuracy; corrected SQL upsert snippet |
| **Benchmarks** | "Measure wall-clock time for 1K, 10K, 100K records" | Generated benchmark script | Ran benchmarks, recorded actual values: 0.32s / 3.26s / 37.8s |

### Where AI Accelerated My Work
- Boilerplate ORM model generation (saved ~2 hours)
- Initial test fixture structure
- Mermaid diagram syntax

### Where I Relied Heavily on AI
- Initial architecture proposal (then significantly modified)
- SQLAlchemy 2.0 `Session.execute()` upsert patterns

### What I Personally Reviewed, Changed, or Validated
- **Every test**: ran, debugged, and verified expected values
- **Reconciliation math**: verified `Input = Valid + Rejected + Duplicates` holds across all 100K benchmarks
- **Idempotency**: manually re-ran the same pipeline twice and confirmed `inserted=0, updated=920`
- **Quarantine payload**: confirmed raw JSON is fully preserved (no field truncation)
- **Line ending normalization**: identified and fixed CRLF issue on Windows → added second commit
- **`.venv` with system-site-packages**: resolved pytest module-not-found issue in sandboxed env

---

## 04 · MAKE YOUR REASONING VISIBLE — Design Walkthrough

### Problem Statement
Upstream CSV feeds arrive with schema mutations, missing values, corrupt strings, and business rule violations. Silently dropping records causes **data leakage**. Crashing the pipeline causes **downstream outages**. The solution must be auditable, replayable, and idempotent.

### Key Assumptions
1. `customer_id` is the natural business key (deterministic upsert anchor)
2. Phone and country_code are nullable (not all customers provide them)
3. `registration_date` cannot be in the future (business invariant, not a schema constraint)
4. A "run" is one CSV file ingestion; each run gets a UUID for lineage tracing
5. Invalid records must never be silently dropped — quarantine is mandatory

### Key Decisions & Trade-offs

| Decision | Chosen | Alternative Considered | Why |
|---|---|---|---|
| **Storage** | SQLite (local) + PostgreSQL (docker) | In-memory only | SQLite gives persistent auditable state without infrastructure; Postgres path exists for production |
| **Validation library** | Pydantic v2 | Pandera / Great Expectations | Pydantic gives field-level error messages, is fast (Rust core), and has zero extra infra |
| **Ingestion** | Pandas chunked streaming | Row-by-row iteration | C-based CSV parsing + constant memory via `chunksize=2500`; handles 100K in <40s |
| **Deduplication** | In-batch `seen_ids` set | Cross-batch DB lookup | O(1) per record; cross-batch dupes handled by upsert idempotency |
| **Upsert** | `ON CONFLICT DO UPDATE` | `DELETE + INSERT` | True idempotency: re-running updates existing rows, never creates duplicates |
| **Quarantine storage** | Full raw JSON payload | Only error code | 100% preservation allows operators to correct and replay without re-fetching source |
| **Streaming/orchestration** | Pure Python | Kafka / Airflow / Spark | Zero-dependency, runs in <1 minute from clone; complexity added when scale demands it |

### Testing Approach

8 test files, each covering one architectural layer:

| File | Layer | Key Assertions |
|---|---|---|
| `test_ingestion.py` | CSV reader | Chunking, lineage injection, empty/header-only/missing file |
| `test_validation.py` | Validators | Each error code triggered in isolation |
| `test_duplicates.py` | Dedup | Intra-batch duplicate detection |
| `test_quarantine.py` | Quarantine | Full payload preservation, state transition |
| `test_replay.py` | Replay engine | Success path and retry-increment failure path |
| `test_idempotency.py` | Upsert | Re-running same batch = 0 new rows |
| `test_pipeline.py` | E2E integration | Mixed dataset, reconciliation equation |
| `test_cli.py` | CLI | Settings loading, ingest + reconcile subcommands |

All 23 tests pass in **~3.5 seconds** using isolated in-memory SQLite fixtures (no shared state between tests).

### Limitations & Future Improvements

| Limitation | Future Fix |
|---|---|
| Single-node chunking | Apache Kafka + PySpark for sub-second streaming at TB scale |
| No operator web UI | Streamlit dashboard for reviewing and one-click correcting quarantine records |
| No SCD Type 2 | Add `valid_from`/`valid_to`/`is_current` for full temporal history |
| `ResourceWarning` on SQLite connection close | Add explicit `engine.dispose()` in test teardown (cosmetic, not functional) |
| `2574b67` commit is a chore, not a feature | For production: use pre-commit hook for line endings |

### How I Know It Works

1. **Reconciliation equation always passes**: `Input (1000) = Valid (920) + Rejected (60) + Duplicates (20)`
2. **23/23 tests green** at 91% coverage
3. **Idempotency verified**: second pipeline run on same file → `Inserted: 0, Updated: 920`
4. **Referential integrity**: `Orphan Referential Keys: 0` in reconcile report
5. **Benchmarks match README claims**: 100K records processed in 37.8s ≈ 2,644 rec/s

---

## Actual Verified Output (13 Nov 2026)

```
============================================================
PIPELINE RUN REPORT
============================================================
Run ID              : RUN-20261113-071105-4A5F5C
Source File         : customers.csv

Input Records       : 1000
Valid Records       : 920
  - Target Inserted : 0
  - Target Updated  : 920
Rejected Records    : 60
Duplicate Records   : 20

Processing Time     : 0.2906 seconds
Throughput          : 3440.7 records/sec
Status              : PARTIAL_SUCCESS

============================================================
QUARANTINE SUMMARY
============================================================
MISSING_REQUIRED_FIELD : 21
DUPLICATE_RECORD       : 20
INVALID_EMAIL          : 7
FUTURE_DATE            : 7
INVALID_STATUS         : 7
MALFORMED_RECORD       : 6
INVALID_CUSTOMER_ID    : 6
INVALID_REFERENCE      : 6

============================================================
RECONCILIATION
============================================================
Equation: Input = Valid + Rejected + Duplicate
1000 = 920 + 60 + 20
Result: PASS
============================================================
```

```
pytest --cov=src:  23 passed, 91% coverage  (3.57s)
benchmark 100K  :  37.8s → 2,644 rec/s
```

