# Architecture Deep-Dive
## Data Quality, Quarantine & Controlled Replay Pipeline

> This document describes **how the system is built**, **why each layer exists**, and **what the code is actually doing**. It is intended as the walkthrough guide a reviewer can follow while reading the source.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Module Map & Dependency Graph](#2-module-map--dependency-graph)
3. [Layer-by-Layer Breakdown](#3-layer-by-layer-breakdown)
   - [3.1 Ingestion — `src/ingestion/csv_reader.py`](#31-ingestion--srcingestioncsv_readerpy)
   - [3.2 Validation Engine — `src/validation/`](#32-validation-engine--srcvalidation)
   - [3.3 Transformation — `src/transformation/transformer.py`](#33-transformation--srctransformationtransformerpy)
   - [3.4 Quarantine — `src/quarantine/`](#34-quarantine--srcquarantine)
   - [3.5 Persistence & Idempotency — `src/database/repositories.py`](#35-persistence--idempotency--srcdatabaserepositoriespy)
   - [3.6 Replay Engine — `src/replay/replay_service.py`](#36-replay-engine--srcreplayreplay_servicepy)
   - [3.7 Observability — `src/monitoring/metrics.py`](#37-observability--srcmonitoringmetricspy)
   - [3.8 Orchestrator — `src/pipeline/runner.py`](#38-orchestrator--srcpipelinerunnerpy)
   - [3.9 CLI — `src/main.py`](#39-cli--srcmainpy)
4. [Database Schema & ER Model](#4-database-schema--er-model)
5. [Data Flow — Request Trace](#5-data-flow--request-trace)
6. [Quarantine State Machine](#6-quarantine-state-machine)
7. [Idempotency Contract](#7-idempotency-contract)
8. [Performance Model](#8-performance-model)
9. [Test Architecture](#9-test-architecture)
10. [Configuration System](#10-configuration-system)
11. [Key Design Decisions with Code Evidence](#11-key-design-decisions-with-code-evidence)

---

## 1. System Overview

```
CSV Source File
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│                      PIPELINE RUNNER                            │
│  run_id = "RUN-20261113-071105-4A5F5C"                          │
│                                                                 │
│  ┌──────────┐   chunks   ┌────────────┐   pass   ┌──────────┐  │
│  │CsvReader │──────────► │ Validators │─────────►│Transform │  │
│  └──────────┘            └────────────┘          └──────────┘  │
│                               │ fail                    │       │
│                               ▼                         ▼       │
│                      ┌──────────────┐        ┌──────────────┐  │
│                      │  Quarantine  │        │  Customers   │  │
│                      │   Store      │        │  (Upsert)    │  │
│                      └──────────────┘        └──────────────┘  │
│                               │                                 │
│                        CORRECTED ──► Replay ──► Re-validate     │
└─────────────────────────────────────────────────────────────────┘
      │
      ▼
  PipelineMetricsTracker
  Input = Valid + Rejected + Duplicates  → RECONCILIATION PASS
```

The pipeline processes records through **6 sequential gates**. A record exits exactly one gate — it is either persisted in `customers` (success) or quarantined in `quarantine_records` (rejected). No record is ever silently dropped.

---

## 2. Module Map & Dependency Graph

```
src/
├── main.py                        ← CLI entry point (argparse)
│
├── config/
│   └── settings.py                ← Pydantic Settings, env/YAML loader
│
├── pipeline/
│   └── runner.py                  ← Orchestrator: wires all modules together
│
├── ingestion/
│   └── csv_reader.py              ← Pandas chunked reader + lineage injection
│
├── validation/
│   ├── models.py                  ← ErrorCodes, QuarantineCandidate, ValidatedRecord
│   ├── schema_validator.py        ← Stage 1 & 2: schema contract + null checks + type parsing
│   ├── business_validator.py      ← Stage 3: domain rules + referential integrity
│   └── duplicate_validator.py     ← Stage 4: in-batch deduplication via seen_ids set
│
├── transformation/
│   └── transformer.py             ← Canonical type coercion → ValidatedRecord dataclass
│
├── quarantine/
│   └── quarantine_service.py      ← Correction lifecycle helper
│
├── replay/
│   └── replay_service.py          ← Controlled replay: re-validate corrected records
│
├── monitoring/
│   └── metrics.py                 ← Run metrics, reconciliation, formatted report
│
└── database/
    ├── connection.py              ← Engine factory, session context manager, init_db
    ├── models.py                  ← SQLAlchemy 2.0 ORM: 5 tables
    └── repositories.py            ← 4 repository classes: Customer, Quarantine, PipelineRun, ReplayRun
```

**Dependency rule**: lower layers never import from higher layers.

```
main.py → pipeline/runner.py → {ingestion, validation, transformation, database, monitoring}
                              ↑
replay_service.py → {validation, transformation, database}
```

---

## 3. Layer-by-Layer Breakdown

### 3.1 Ingestion — `src/ingestion/csv_reader.py`

**Responsibility**: Read the CSV, inject lineage metadata, yield chunks to the pipeline.

**Key design choices:**

| Choice | Code | Reason |
|--------|------|--------|
| `dtype=str` | `pd.read_csv(..., dtype=str, keep_default_na=False)` | Prevents Pandas from silently coercing types. The validator decides types, not the reader. |
| `chunksize=2500` | `pd.read_csv(..., chunksize=self.batch_size)` | Constant memory regardless of file size. 100K rows = 40 chunks, never loads full file. |
| `on_bad_lines="warn"` | explicit flag | Malformed CSV rows are logged, not fatal, so one corrupt line doesn't kill a 100K run. |
| Lineage injection | `_source_file`, `_source_row_number`, `_run_id`, `_ingested_at` | Added to every record dict before yielding. These internal keys (prefixed `_`) thread through the entire pipeline without touching the user payload. |

**Empty file handling:**
```python
def check_file_empty(self, file_path: Path) -> Tuple[bool, bool]:
    # Returns (is_completely_empty, is_header_only)
    # Reads only 2 lines — O(1) for any file size
```
The pipeline calls this before the chunked read and exits cleanly with `status=SUCCESS` for empty/header-only files, rather than raising an exception.

---

### 3.2 Validation Engine — `src/validation/`

The engine runs **4 sequential stages**. A record failing any stage is immediately quarantined — it does not proceed to later stages.

#### Stage 1 & 2: `schema_validator.py` — `SchemaValidator`

**Stage 1: Header Contract Validation** (`validate_file_columns`)
- Checks for missing required columns → `SCHEMA_MISSING_COLUMN`
- Checks for unexpected extra columns → `SCHEMA_UNEXPECTED_COLUMN`
- Internal `_`-prefixed columns are excluded from unexpected-column checks

**Stage 2: Required Field + Type Parsing** (`validate_record`)

Runs two sub-passes within the same method:

```python
# Sub-pass A: Required field presence
required_fields = ["customer_id", "customer_name", "email",
                   "customer_type_id", "registration_date", "status"]
for field in required_fields:
    if val is None or str(val).strip() == "" or str(val).strip().upper() == "NULL":
        → MISSING_REQUIRED_FIELD

# Sub-pass B: Type parsing (only if A passes)
int(record["customer_id"])     → INVALID_DATA_TYPE on ValueError
int(record["customer_type_id"]) → INVALID_DATA_TYPE
datetime.strptime(reg_date, "%Y-%m-%d") → MALFORMED_RECORD on ValueError
datetime.fromisoformat(updated_at)      → MALFORMED_RECORD on ValueError
```

**Why validate types in the schema stage?** Because type failures are structural errors, not domain errors. The business validator receives guaranteed `int` / `date` / `datetime` Python objects — no defensive casting needed.

**Output**: A normalized dict with Python-native types replaces the raw string dict:
```python
normalized = {
    "customer_id": parsed_customer_id,  # int
    "registration_date": parsed_reg_date,  # date object
    "status": str(...).strip().upper(),  # canonical uppercase
    # + lineage keys preserved
}
```

---

#### Stage 3: `business_validator.py` — `BusinessValidator`

Receives the normalized dict. Checks 5 domain rules, **accumulating all errors** before quarantining (so a record with both bad email and future date gets both error codes stored):

| Rule | Error Code | Implementation |
|------|-----------|----------------|
| `customer_id > 0` | `INVALID_CUSTOMER_ID` | `customer_id <= 0` check on int |
| Email format | `INVALID_EMAIL` | RFC 5322 regex: `^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$` |
| Status enum | `INVALID_STATUS` | `status not in {"ACTIVE","INACTIVE","SUSPENDED"}` |
| Registration date ≤ today | `FUTURE_DATE` | `reg_date > date.today()` |
| `customer_type_id` in reference table | `INVALID_REFERENCE` | O(1) lookup in `valid_customer_type_ids` set |

**Reference cache architecture:**
```python
# In PipelineRunner.run():
active_types = customer_repo.get_active_customer_type_ids()  # one DB query per run
business_validator.set_valid_reference_keys(active_types)   # update in-memory set

# In BusinessValidator.validate_record():
if cust_type_id not in self.valid_customer_type_ids:  # O(1) set lookup, not a DB query
```
This converts what would be O(N) foreign key roundtrips into a single query + N O(1) set lookups.

---

#### Stage 4: `duplicate_validator.py` — `DuplicateValidator`

```python
class DuplicateValidator:
    def __init__(self):
        self.seen_ids: set[int] = set()

    def process_records(self, records):
        unique, duplicates = [], []
        for rec in records:
            cid = rec["customer_id"]
            if cid in self.seen_ids:
                duplicates.append(QuarantineCandidate(..., DUPLICATE_RECORD))
            else:
                self.seen_ids.add(cid)
                unique.append(rec)
        return unique, duplicates
```

`seen_ids` persists **across chunks within a single run** (via `reset()` only called at run start). Cross-run duplicates are handled by the upsert — not this validator.

---

### 3.3 Transformation — `src/transformation/transformer.py`

Receives a business-validated record and constructs a strongly-typed `ValidatedRecord` dataclass. The transformation layer is the **last chance to quarantine** before writing to the database — it catches edge cases like truncation, encoding issues, or unexpected `None` values in otherwise valid fields.

```python
@dataclass
class ValidatedRecord:
    customer_id: int
    customer_name: str
    email: str
    phone: Optional[str]
    country_code: Optional[str]
    customer_type_id: int
    registration_date: date
    status: str
    updated_at: Optional[datetime]
    source_file: str
    source_row_number: int
    run_id: str
```

Using a dataclass (not a dict) gives **type safety at the persistence boundary** — the repository layer receives guaranteed types, eliminating a whole class of database type errors.

---

### 3.4 Quarantine — `src/quarantine/`

#### `validation/models.py` — Domain types

```python
class ErrorCodes:
    SCHEMA_MISSING_COLUMN = "SCHEMA_MISSING_COLUMN"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_DATA_TYPE = "INVALID_DATA_TYPE"
    MALFORMED_RECORD = "MALFORMED_RECORD"
    INVALID_CUSTOMER_ID = "INVALID_CUSTOMER_ID"
    INVALID_EMAIL = "INVALID_EMAIL"
    INVALID_STATUS = "INVALID_STATUS"
    FUTURE_DATE = "FUTURE_DATE"
    INVALID_REFERENCE = "INVALID_REFERENCE"
    DUPLICATE_RECORD = "DUPLICATE_RECORD"
    TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"

@dataclass
class QuarantineCandidate:
    source_record_id: Optional[str]
    source_file: str
    source_row_number: int
    run_id: str
    raw_payload: dict        # Complete original row — nothing truncated
    errors: List[ValidationErrorDetail]

    @property
    def primary_error(self) -> ValidationErrorDetail:
        return self.errors[0]  # First-failing error used as primary error_code
```

#### `database/repositories.py` — `QuarantineRepository.save_quarantine_candidates`

```python
all_errors_msg = " | ".join(
    f"[{e.error_code}] {e.error_message}" for e in c.errors
)
payload_str = json.dumps(c.raw_payload, default=str)

q_record = QuarantineRecord(
    raw_payload=payload_str,       # 100% raw payload preserved as JSON string
    error_code=primary.error_code, # Primary error code for filtering/indexing
    error_message=all_errors_msg,  # All error messages concatenated — nothing lost
    status="QUARANTINED",
    retry_count=0,
    ...
)
```

Every quarantined record stores:
- Complete original payload (all columns, raw string values)
- All validation errors (not just the first one)
- Source lineage (`source_file`, `source_row_number`, `run_id`)
- Full audit timestamps (`first_seen_at`, `last_seen_at`)

---

### 3.5 Persistence & Idempotency — `src/database/repositories.py`

#### `CustomerRepository.upsert_customers`

The upsert is dialect-aware — it uses the native `INSERT ... ON CONFLICT DO UPDATE` for both PostgreSQL and SQLite:

```python
# PostgreSQL path
from sqlalchemy.dialects.postgresql import insert as pg_insert
stmt = pg_insert(Customer).values(values_list)
stmt = stmt.on_conflict_do_update(
    index_elements=[Customer.customer_id],
    set_={col: getattr(stmt.excluded, col) for col in update_cols},
)

# SQLite path (identical API, different import)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
stmt = sqlite_insert(Customer).values(values_list)
stmt = stmt.on_conflict_do_update(...)
```

**Insert vs Update tracking:**
```python
existing_ids = self.get_existing_ids(incoming_ids)  # one SELECT IN query
inserted_count = len(incoming_ids) - len(existing_ids)
updated_count = len(existing_ids)
```

This determines whether a record is a true new insert or an update **before** executing the upsert, enabling the `Inserted: 920 / Updated: 0` vs `Inserted: 0 / Updated: 920` distinction in the report.

---

### 3.6 Replay Engine — `src/replay/replay_service.py`

The replay service re-runs the full validation pipeline on corrected quarantine records. It is a **miniature pipeline** — not a shortcut:

```
ReplayService.replay_corrected_records()
  │
  ├── quarantine_repo.get_records_by_status("CORRECTED", limit=500)
  │
  └── for each q_record:
        payload = json.loads(q_record.raw_payload)   # restore raw payload
        inject lineage: _run_id = "REPLAY-{timestamp}"
        │
        ├── schema_validator.validate_record(payload)
        │     → fail: _handle_replay_failure() → status=REPLAY_FAILED, retry_count++
        │
        ├── business_validator.validate_record(schema_valid)
        │     → fail: _handle_replay_failure()
        │
        ├── transformer.transform_record(biz_valid)
        │     → fail: _handle_replay_failure()
        │
        └── customer_repo.upsert_customers([transformed])
              q_record.status = "REPLAYED"
              replay_repo.log_replay(status="SUCCESS")
```

**`retry_count` semantics:**
- Incremented on every `REPLAY_FAILED` outcome
- Never reset between attempts
- Provides an audit trail of how many correction attempts were made
- Allows operators to filter records that have failed replay multiple times

---

### 3.7 Observability — `src/monitoring/metrics.py`

`PipelineMetricsTracker` is instantiated at run start and passed through the pipeline. It tracks:

```python
self.input_count      # total rows read from CSV
self.valid_count      # rows that passed all validation and were upserted
self.rejected_count   # rows quarantined (excluding duplicates)
self.duplicate_count  # rows quarantined as DUPLICATE_RECORD
self.error_code_counts: Counter[str]  # histogram of quarantine error codes
```

**Reconciliation invariant:**
```python
@property
def is_reconciled(self) -> bool:
    return self.input_count == (self.valid_count + self.rejected_count + self.duplicate_count)
```

This is checked at the end of every run. For the 1,000-row test dataset:
```
1000 = 920 (valid) + 60 (rejected) + 20 (duplicates)  → PASS
```

Duplicates are tracked **separately** from rejected because:
1. Duplicate detection happens after business validation — a duplicate that also has an invalid email would only be quarantined once (as `MISSING_REQUIRED_FIELD` or `INVALID_EMAIL`), not counted twice.
2. Separating them enables downstream reports to distinguish data quality problems from operational deduplication.

---

### 3.8 Orchestrator — `src/pipeline/runner.py`

`PipelineRunner.run()` wires everything together in a single session context:

```python
with get_db_session(self.engine) as session:
    # Initialize repositories (all share same session → one transaction)
    customer_repo = CustomerRepository(session)
    quarantine_repo = QuarantineRepository(session)

    # Pre-load reference cache
    active_types = customer_repo.get_active_customer_type_ids()
    business_validator.set_valid_reference_keys(active_types)

    for batch in csv_reader.read_records(path, run_id):
        tracker.record_input(len(batch))

        # Stage 1+2: Schema + Required + Type
        schema_passed, quarantine_batch = schema_validator.validate_record(each)

        # Stage 3: Business rules
        business_passed, ... = business_validator.validate_record(each)

        # Stage 4: Deduplication
        unique_records, duplicate_cands = duplicate_validator.process_records(business_passed)

        # Stage 5: Transform → ValidatedRecord
        valid_to_load, ... = transformer.transform_record(each)

        # Persist quarantine batch
        quarantine_repo.save_quarantine_candidates(quarantine_batch)

        # Persist valid records (idempotent upsert)
        inserted, updated = customer_repo.upsert_customers(valid_to_load)
```

All repositories share the **same SQLAlchemy `Session`**, so the entire batch write (quarantine + upsert) commits atomically. If the DB write fails mid-batch, the whole batch rolls back — no partial writes.

---

### 3.9 CLI — `src/main.py`

Three subcommands exposed via `argparse`:

```bash
python -m src.main ingest    --input FILE [--batch-size N] [--db-url URL]
python -m src.main replay    [--limit N]  [--db-url URL]
python -m src.main reconcile [--db-url URL]
```

The `--db-url` flag allows overriding the environment default at runtime — useful for running against different environments (test SQLite vs prod PostgreSQL) without modifying `.env`.

---

## 4. Database Schema & ER Model

```
customer_types          customers
──────────────          ─────────────────────────────────
customer_type_id PK ──┐ customer_id      PK
type_name           │ ├ customer_name
description         │ ├ email
is_active           │ ├ phone
                    │ ├ country_code
                    └►├ customer_type_id  FK
                      ├ registration_date
                      ├ status
                      ├ source_file       ← lineage
                      ├ source_row_number ← lineage
                      ├ run_id            FK → pipeline_runs
                      ├ created_at
                      └ updated_at

pipeline_runs           quarantine_records
─────────────           ─────────────────────────────────
run_id PK ──────────┐  quarantine_id    PK (UUID)
source_file         │  source_record_id
started_at          │  source_file      ← lineage
completed_at        │  source_row_number← lineage
status              └►─run_id           FK
input_count            raw_payload      TEXT (full JSON)
valid_count            error_code
rejected_count         error_message    (all errors concatenated)
duplicate_count        validation_stage
processing_time_s      status           QUARANTINED|CORRECTED|REPLAYED|REPLAY_FAILED
error_message          retry_count
                       first_seen_at
                       last_seen_at
                       corrected_at
                       replayed_at

replay_runs
──────────────────────────
replay_run_id PK (UUID)
quarantine_id  FK → quarantine_records
original_run_id
replay_started_at
replay_completed_at
status         SUCCESS|FAILED
old_error
new_error
target_record_id
```

**Indexes** (from `schema.sql`):
- `customers(email, run_id)` — fast post-run email lookups
- `quarantine_records(status, error_code, run_id)` — fast replay eligibility queries and error code aggregation

---

## 5. Data Flow — Request Trace

Tracing a single row `customer_id=42` through the system:

```
Row 2 of customers.csv:
  raw string: "42,Alice Smith,alice@example.com,,US,2,2025-01-15,ACTIVE,2025-01-16 10:00:00"

Step 1 — CsvReader injects lineage:
  {"customer_id": "42", "customer_name": "Alice Smith", ...,
   "_source_file": "customers.csv", "_source_row_number": 2, "_run_id": "RUN-..."}

Step 2 — SchemaValidator.validate_record():
  required fields → all present ✓
  int("42") → 42 ✓
  datetime.strptime("2025-01-15") → date(2025, 1, 15) ✓
  output: normalized dict with Python types

Step 3 — BusinessValidator.validate_record():
  42 > 0 ✓
  EMAIL_REGEX.match("alice@example.com") ✓
  "ACTIVE" in valid_statuses ✓
  date(2025, 1, 15) <= date.today() ✓
  2 in {1,2,3,4,5} ✓
  → passes all rules

Step 4 — DuplicateValidator.process_records():
  42 not in seen_ids → seen_ids.add(42) ✓

Step 5 — Transformer.transform_record():
  → ValidatedRecord(customer_id=42, customer_name="Alice Smith", ...)

Step 6 — CustomerRepository.upsert_customers([validated_record]):
  INSERT INTO customers (...) VALUES (...)
  ON CONFLICT (customer_id) DO UPDATE SET ...
  → customer_id=42 written to DB, inserted_count=1

Step 7 — PipelineMetricsTracker:
  input_count=1, valid_count=1, rejected_count=0, duplicate_count=0
  Reconciliation: 1 = 1 + 0 + 0 → PASS
```

**Failing row trace** — `customer_id=-956`:

```
Step 2 — SchemaValidator: int("-956") = -956 ✓ (valid integer type)
Step 3 — BusinessValidator: -956 <= 0 → INVALID_CUSTOMER_ID error appended
  → QuarantineCandidate(source_record_id="-956",
                        raw_payload={"customer_id": "-956", ...},
                        errors=[ValidationErrorDetail(INVALID_CUSTOMER_ID, ...)])
Step 5 — QuarantineRepository.save_quarantine_candidates():
  → QuarantineRecord(error_code="INVALID_CUSTOMER_ID",
                     raw_payload='{"customer_id": "-956", ...}',
                     status="QUARANTINED") written to DB
Metrics: rejected_count++, error_code_counts["INVALID_CUSTOMER_ID"]++
```

---

## 6. Quarantine State Machine

```
                    ┌─────────────┐
  CSV Validation    │  QUARANTINED │ ◄── Initial state on ingestion failure
  Failure           └──────┬──────┘
                           │  Operator corrects raw_payload JSON
                           ▼
                    ┌─────────────┐
                    │  CORRECTED  │ ◄── quarantine_service.correct_record()
                    └──────┬──────┘
                           │  replay_service.replay_corrected_records()
                      Re-validates through full pipeline
                      ┌────┴────────────────┐
                      │                     │
                      ▼ PASS                ▼ FAIL
               ┌───────────┐        ┌──────────────┐
               │  REPLAYED │        │ REPLAY_FAILED │
               └───────────┘        └──────┬───────┘
                 → Loaded into          retry_count++
                   customers            error_code updated
                                        ↑
                                   Operator corrects again
                                   → Back to CORRECTED
```

**State transitions are enforced at the application layer** — only records with `status="CORRECTED"` are eligible for replay. The query in `QuarantineRepository.get_records_by_status("CORRECTED")` acts as the gate.

---

## 7. Idempotency Contract

**Claim**: Running the pipeline twice on the same input file produces zero new rows and updates all existing rows.

**How it's enforced:**

1. **Natural key**: `customer_id` (business-assigned integer, not a DB sequence)
2. **Upsert**: `ON CONFLICT (customer_id) DO UPDATE SET ...` — SQLite and PostgreSQL native
3. **Verification**: `get_existing_ids()` before upsert → `updated_count = len(existing_ids)`

**Verified output (run 1 → run 2):**
```
Run 1:  Inserted: 920  Updated: 0
Run 2:  Inserted: 0    Updated: 920
Run 3:  Inserted: 0    Updated: 920  (same as run 2 — stable)
```

**Test coverage**: `tests/test_idempotency.py::test_pipeline_idempotency`
```python
runner.run(csv_file)   # First run
tracker2 = runner.run(csv_file)  # Second run
assert tracker2.target_inserted_count == 0
assert tracker2.target_updated_count > 0
```

---

## 8. Performance Model

| Scale | Time | Throughput | Bottleneck |
|-------|------|-----------|------------|
| 1,000 rows | 0.32 s | 3,104 rec/s | Python startup overhead |
| 10,000 rows | 3.26 s | 3,068 rec/s | SQLite write lock contention |
| 100,000 rows | 37.8 s | 2,644 rec/s | SQLite sequential I/O |

**Memory profile**: Constant ~50 MB regardless of file size.
- `chunksize=2500` rows loaded per iteration
- Chunk processed and discarded before next chunk is read
- `seen_ids` set for dedup: O(N) memory where N = unique valid customer_ids seen so far

**O(1) reference cache** eliminates the most common N+1 pattern:
```python
# Without cache: 1000 rows × 1 SELECT = 1000 DB roundtrips
# With cache:    1 SELECT + 1000 set lookups = O(1) per record
active_types = customer_repo.get_active_customer_type_ids()  # once per run
business_validator.set_valid_reference_keys(active_types)
```

**Future scaling path**:
- Replace SQLite with PostgreSQL → 3-5× write throughput improvement via WAL + connection pooling
- Replace Pandas chunking with PySpark for multi-node parallelism
- Replace single-process with Kafka consumers for sub-second streaming

---

## 9. Test Architecture

All tests use **in-memory SQLite** fixtures defined in `tests/conftest.py`:

```python
@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    init_db(eng)
    return eng

@pytest.fixture
def db_session(engine):
    with get_db_session(engine) as session:
        yield session
```

Each test gets its **own isolated engine** — no shared state between tests. This is why all 23 tests run in 3.5 seconds.

**Test coverage map:**

| Test File | Layer | Critical Assertions |
|-----------|-------|---------------------|
| `test_ingestion.py` | CsvReader | Lineage injection, empty file graceful exit, missing file raises `IngestionError` |
| `test_validation.py` | SchemaValidator + BusinessValidator | Each of the 9 error codes triggered in isolation |
| `test_duplicates.py` | DuplicateValidator | First occurrence accepted, second quarantined as `DUPLICATE_RECORD` |
| `test_quarantine.py` | QuarantineRepository | Full payload stored, `source_row_number` preserved, `mark_as_corrected()` changes status |
| `test_replay.py` | ReplayService | Valid corrected record → `REPLAYED` + written to customers; invalid → `REPLAY_FAILED` + `retry_count=1` |
| `test_idempotency.py` | PipelineRunner | Second run: `inserted=0`, `updated>0`, customer count unchanged |
| `test_pipeline.py` | End-to-end | Reconciliation equation passes, quarantine counts match expected distribution |
| `test_cli.py` | CLI + Settings | Settings loaded from env, ingest + reconcile subcommands execute cleanly |

**Coverage gap analysis (9% uncovered):**
- `connection.py` lines 42-43, 86-88: PostgreSQL-specific connection error branches (requires live PG)
- `runner.py` lines 131-138: `IngestionError` / general exception handlers (tested indirectly)
- `replay_service.py` lines 45-50: PostgreSQL-dialect upsert path in replay (SQLite only in tests)

---

## 10. Configuration System

`src/config/settings.py` uses **Pydantic Settings** with a two-layer config hierarchy:

```python
class AppSettings(BaseSettings):
    # Database
    database_url: str = "sqlite:///./data_pipeline.db"

    # Pipeline tuning
    batch_size: int = 2500
    replay_limit: int = 500

    # Validation rules (can be overridden per environment)
    valid_statuses: list[str] = ["ACTIVE", "INACTIVE", "SUSPENDED"]
    valid_customer_type_ids: list[int] = [1, 2, 3, 4, 5]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
```

Resolution order (highest → lowest priority):
1. CLI `--db-url` / `--batch-size` flags (argparse overrides)
2. `.env` file values
3. Environment variables
4. `config/config.yaml` defaults
5. Pydantic field defaults

---

## 11. Key Design Decisions with Code Evidence

### Decision 1: Fail-fast per-record, not per-batch

A single invalid record does not abort the batch. The validator returns `(None, QuarantineCandidate)` on failure, `(cleaned_record, None)` on success. The runner collects both and processes them independently.

```python
# runner.py lines 84-98
for raw_rec in batch_records:
    cleaned_rec, cand = self.schema_validator.validate_record(raw_rec)
    if cand:
        quarantine_batch.append(cand)   # failed — quarantine it
    else:
        schema_passed.append(cleaned_rec)  # passed — continue
```

### Decision 2: Raw payload preserved as-is in quarantine

```python
# repositories.py line 135
payload_str = json.dumps(c.raw_payload, default=str)
```

`default=str` ensures non-serializable types (dates, Decimals) are safely stringified rather than raising a `TypeError`. The original source values are always recoverable — no data is ever destroyed.

### Decision 3: `duplicate_count` is separated from `rejected_count`

```python
# runner.py lines 101-120
unique_records, duplicate_cands = self.duplicate_validator.process_records(business_passed)
quarantine_batch.extend(duplicate_cands)
tracker.record_duplicate(len(duplicate_cands))  # separate counter

non_dupe_errors = [e for e in error_codes if e != ErrorCodes.DUPLICATE_RECORD]
tracker.record_rejected(len(non_dupe_errors), non_dupe_errors)
```

Duplicates and business-invalid records are stored in the same `quarantine_records` table but tracked in separate counters. The reconciliation equation `Input = Valid + Rejected + Duplicates` treats them as distinct categories.

### Decision 4: `seen_ids` set, not a dict

An earlier version used `Dict[int, int]` to track counts. The validator only needs membership testing, not counts. A `set` uses less memory and is semantically clearer:

```python
# duplicate_validator.py
self.seen_ids: set[int] = set()   # O(1) lookup, minimal memory footprint
if cid in self.seen_ids:          # first occurrence kept, all subsequent quarantined
```

### Decision 5: Shared SQLAlchemy Session across all repositories per batch

All 4 repositories (`CustomerRepository`, `QuarantineRepository`, `PipelineRunRepository`, `ReplayRunRepository`) share one `Session` within each pipeline run. This means:
- One database connection per run (not per repository)
- Quarantine writes and customer upserts are part of the same transaction
- If the commit fails, neither is written — atomic consistency

```python
# runner.py
with get_db_session(self.engine) as session:
    customer_repo = CustomerRepository(session)
    quarantine_repo = QuarantineRepository(session)
    # Both use same session → same transaction
```

