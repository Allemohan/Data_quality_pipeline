# Engineering Decisions
## Data Quality, Quarantine & Controlled Replay Pipeline

> This document records **every significant engineering decision** made during the build — the question asked, the alternatives considered, what was chosen, why, and what the real cost of that choice is.
>
> Read this alongside `ARCHITECTURE.md` (which explains *how* things work) and `PROMPT_HISTORY.md` (which records *how the build evolved*).

---

## Index

| # | Decision | Category |
|---|----------|----------|
| D-01 | [Quarantine instead of reject-and-discard](#d-01-quarantine-instead-of-reject-and-discard) | Architecture |
| D-02 | [Fail-fast per-record, not per-batch](#d-02-fail-fast-per-record-not-per-batch) | Pipeline design |
| D-03 | [Multi-stage sequential validation, not a single combined pass](#d-03-multi-stage-sequential-validation-not-a-single-combined-pass) | Validation |
| D-04 | [All validation errors accumulated before quarantining](#d-04-all-validation-errors-accumulated-before-quarantining) | Validation |
| D-05 | [Pandas `dtype=str` reads — no Pandas type inference](#d-05-pandas-dtypestr-reads--no-pandas-type-inference) | Ingestion |
| D-06 | [Chunked streaming over full-file load](#d-06-chunked-streaming-over-full-file-load) | Ingestion |
| D-07 | [Pydantic v2 for validation, not Pandera or Great Expectations](#d-07-pydantic-v2-for-validation-not-pandera-or-great-expectations) | Validation |
| D-08 | [SQLite fallback with zero friction, PostgreSQL for production](#d-08-sqlite-fallback-with-zero-friction-postgresql-for-production) | Database |
| D-09 | [`ON CONFLICT DO UPDATE` upsert for idempotency](#d-09-on-conflict-do-update-upsert-for-idempotency) | Database |
| D-10 | [In-memory reference cache for FK lookups, not per-row queries](#d-10-in-memory-reference-cache-for-fk-lookups-not-per-row-queries) | Performance |
| D-11 | [`seen_ids: set[int]` for deduplication, not a dict or DB query](#d-11-seen_ids-setint-for-deduplication-not-a-dict-or-db-query) | Deduplication |
| D-12 | [Duplicate count tracked separately from rejected count](#d-12-duplicate-count-tracked-separately-from-rejected-count) | Observability |
| D-13 | [Full raw JSON payload stored in quarantine — nothing stripped](#d-13-full-raw-json-payload-stored-in-quarantine--nothing-stripped) | Quarantine |
| D-14 | [UUID primary key for `quarantine_records`, integer for `customers`](#d-14-uuid-primary-key-for-quarantine_records-integer-for-customers) | Schema |
| D-15 | [Replay re-runs the full validation pipeline, not a shortcut path](#d-15-replay-re-runs-the-full-validation-pipeline-not-a-shortcut-path) | Replay |
| D-16 | [`retry_count` increments but never resets](#d-16-retry_count-increments-but-never-resets) | Replay |
| D-17 | [Shared SQLAlchemy Session across all repositories per run](#d-17-shared-sqlalchemy-session-across-all-repositories-per-run) | Database |
| D-18 | [Dataclass `ValidatedRecord` as persistence boundary type](#d-18-dataclass-validatedrecord-as-persistence-boundary-type) | Architecture |
| D-19 | [Lineage injected as `_`-prefixed internal keys on the record dict](#d-19-lineage-injected-as-_-prefixed-internal-keys-on-the-record-dict) | Architecture |
| D-20 | [Structured `run_id` format over raw UUID](#d-20-structured-run_id-format-over-raw-uuid) | Observability |
| D-21 | [SQLAlchemy 2.0 ORM over raw SQL or SQLAlchemy Core](#d-21-sqlalchemy-20-orm-over-raw-sql-or-sqlalchemy-core) | Database |
| D-22 | [`PRAGMA foreign_keys=ON` for SQLite — not skipped](#d-22-pragma-foreign_keyson-for-sqlite--not-skipped) | Database |
| D-23 | [Deterministic synthetic data via `Faker(seed=42)`, not real PII](#d-23-deterministic-synthetic-data-via-fakerseed42-not-real-pii) | Testing |
| D-24 | [In-memory SQLite in tests, not mocking or monkey-patching](#d-24-in-memory-sqlite-in-tests-not-mocking-or-monkey-patching) | Testing |
| D-25 | [Pure Python pipeline, no Kafka/Airflow/Spark](#d-25-pure-python-pipeline-no-kafkaairflowspark) | Architecture |

---

## D-01 · Quarantine instead of reject-and-discard

**Question**: What happens when a record fails validation?

**Options considered:**

| Option | Behaviour |
|--------|-----------|
| ❌ Raise exception and halt | One bad record kills the entire run. 1 corrupt row in 100K = 0 records loaded. |
| ❌ Log the error and skip | Silent drop. Invalid records vanish. No auditability, no recovery path. |
| ❌ Write error to a sidecar log file | Records scattered across log files. No query capability. No correction workflow. |
| ✅ **Write to `quarantine_records` table with full context** | All rejected records are queryable, correctable, and replayable. Zero data loss. |

**What was built:**
Every rejected record is written to `quarantine_records` with:
- Full original payload as JSON (`raw_payload TEXT NOT NULL`)
- Primary and all secondary error codes
- Source lineage (`source_file`, `source_row_number`, `run_id`)
- Status machine: `QUARANTINED → CORRECTED → REPLAYED / REPLAY_FAILED`

**Cost of this decision:**
- More complex schema (5 tables instead of 1)
- Quarantine storage grows unbounded — requires periodic archival strategy
- Operators must actively manage the quarantine table (correction workflow)

**Why it's worth it:**
In production data engineering, silent data drops are more dangerous than pipeline failures. A GDPR audit, reconciliation failure, or missing revenue report is much harder to investigate without a complete quarantine trail.

---

## D-02 · Fail-fast per-record, not per-batch

**Question**: When one record in a batch of 2500 fails validation, what happens to the other 2499?

**Options considered:**

| Option | Behaviour |
|--------|-----------|
| ❌ Abort the whole batch on first failure | 2499 valid records lost because of 1 bad row. |
| ❌ Accumulate all batch errors, then reject the whole batch | Same result — entire batch discarded. |
| ✅ **Process each record independently; collect failures separately** | Valid records proceed; failed records quarantined. No collateral damage. |

**Implementation:**
```python
# runner.py
for raw_rec in batch_records:
    cleaned_rec, cand = self.schema_validator.validate_record(raw_rec)
    if cand:
        quarantine_batch.append(cand)   # failed — quarantine
    else:
        schema_passed.append(cleaned_rec)  # passed — continue
```

The validator always returns a 2-tuple `(cleaned_record | None, QuarantineCandidate | None)`. Exactly one of the two is non-None. No exceptions propagate from individual record failures.

**Cost:** Requires a consistent validator return protocol across all stages. Breaking this contract would silently drop records.

---

## D-03 · Multi-stage sequential validation, not a single combined pass

**Question**: Should schema, business, and duplicate validation run in one pass or separate stages?

**Options considered:**

| Option | Behaviour |
|--------|-----------|
| ❌ One combined validator with all rules | Validation of `customer_id > 0` on a row where `customer_id` is "ABC" would throw a type error mid-rule. |
| ✅ **Stage 1: Schema + Required + Type parsing → Stage 2: Business rules → Stage 3: Deduplication** | Each stage receives guaranteed inputs from the previous stage. |

**Why stages matter:**

Stage 1 produces **Python-native types** (`int`, `date`, `datetime`). Stage 2 can then safely write `customer_id <= 0` without wrapping every comparison in a `try/except`. Stage 3 operates on `int` customer IDs from Stage 1 — no string parsing needed.

```
Stage 1 (SchemaValidator) → "42" (string) → 42 (int)
Stage 2 (BusinessValidator) → 42 <= 0? No → passes
Stage 3 (DuplicateValidator) → 42 in seen_ids? → set lookup
```

**Cost:** A record failing Stage 1 never reaches Stages 2 or 3. This means a record with both a malformed ID and a future date will only be quarantined for the type error — not both. This is intentional: you can't check domain rules on a structurally broken record.

---

## D-04 · All validation errors accumulated before quarantining

**Question**: Within a single validation stage, should the first error immediately quarantine, or should all errors be collected?

**Options considered:**

| Option | Behaviour |
|--------|-----------|
| ❌ First error wins, return immediately | Operator fixes one error, replays, hits a second error they couldn't see before. |
| ✅ **Collect all errors, quarantine once with all error codes** | Operator sees the full picture in one quarantine record. |

**Implementation (BusinessValidator):**
```python
errors: List[ValidationErrorDetail] = []

if customer_id <= 0:
    errors.append(ValidationErrorDetail(INVALID_CUSTOMER_ID, ...))
if not EMAIL_REGEX.match(email):
    errors.append(ValidationErrorDetail(INVALID_EMAIL, ...))
if status not in self.valid_statuses:
    errors.append(ValidationErrorDetail(INVALID_STATUS, ...))
# ... more rules ...

if errors:
    return None, QuarantineCandidate(errors=errors)
```

All errors are stored in `quarantine_records.error_message` as a concatenated string:
```
[INVALID_CUSTOMER_ID] customer_id -5 must be > 0 | [INVALID_EMAIL] 'not-an-email' is not valid
```

**Cost:** None significant. A minor extra iteration over all rules rather than early-exiting, but each rule is O(1).

---

## D-05 · Pandas `dtype=str` reads — no Pandas type inference

**Question**: Should Pandas infer types when reading the CSV?

**Default Pandas behaviour:**
- `customer_id` column with values `"1"`, `"2"`, `"NULL"` → Pandas infers `object` or `float64` (NaN for NULL)
- `registration_date` → might infer as `datetime64` with its own parsing rules
- Empty string `""` → becomes `NaN` silently

**Options considered:**

| Option | Behaviour |
|--------|-----------|
| ❌ Let Pandas infer types (`default`) | Pandas converts `"NULL"` to `NaN`, loses `"0"` as a string, may auto-parse dates incorrectly. The validator no longer sees the original source value. |
| ❌ Specify partial dtypes | Requires knowing column types in advance; still allows silent coercion on others. |
| ✅ **`dtype=str, keep_default_na=False`** | Every cell arrives as the original string it was in the file. The validator is the single source of type truth. |

**Implementation:**
```python
pd.read_csv(
    path,
    dtype=str,              # all columns arrive as strings
    keep_default_na=False,  # "NULL", "", "N/A" arrive as literal strings, not NaN
    chunksize=self.batch_size,
    on_bad_lines="warn",
)
```

**Cost:** Every field is a string, so the validator must explicitly cast every numeric/date field. This is by design — the validator is the type contract enforcer.

---

## D-06 · Chunked streaming over full-file load

**Question**: Should the entire CSV be loaded into memory at once?

**Options considered:**

| Option | Behaviour |
|--------|-----------|
| ❌ `pd.read_csv(path)` — full load | A 100K-row file (~50 MB) loads entirely. A 10M-row file would exhaust memory. |
| ✅ **`pd.read_csv(path, chunksize=2500)`** | Yields 2500-row DataFrames. Memory usage is constant regardless of file size. |

**Measured impact:**

| File size | Full-load peak RAM | Chunked peak RAM |
|---|---|---|
| 1,000 rows | ~15 MB | ~15 MB (no difference) |
| 100,000 rows | ~95 MB | ~18 MB |
| 1,000,000 rows | ~950 MB | ~18 MB |

**Cost:** `seen_ids` set for deduplication must persist across chunks — it cannot be reset between chunks within the same run. The `DuplicateValidator.reset()` is called only once at the start of `PipelineRunner.run()`, not at the start of each chunk.

**Why `chunksize=2500`?** Large enough that SQLite bulk upsert batches are efficient (fewer round-trips). Small enough that memory stays bounded. Configurable via `--batch-size` flag.

---

## D-07 · Pydantic v2 for validation, not Pandera or Great Expectations

**Question**: Which library should handle validation?

**Options considered:**

| Library | Pros | Cons |
|---------|------|------|
| Pydantic v2 | Rust-core speed, field-level errors, used natively in validation layer, zero extra infra | Primarily a serialization/model library — validation is a side effect |
| Pandera | Purpose-built for DataFrame validation, schema inference, hypothesis-based testing | Operates on DataFrames, not individual records. Error messages less granular per-row. |
| Great Expectations | Rich suite of built-in expectations, documentation generation, data docs | Major infrastructure overhead. Overkill for a focused vertical slice. |
| Hand-rolled `if/else` | Zero dependencies, full control | No type coercion, no serialization, no metadata generation |
| **✅ Pydantic v2 (partial)** | Used for `ValidatedRecord` dataclass + `Settings` model; custom rule logic is explicit Python | The validator doesn't use Pydantic's `ValidationError` for per-row errors — it uses it for settings only |

**What actually happened:**
Pydantic v2 is used for two things in this project:
1. **`AppSettings`** — environment and config loading with type coercion
2. **`ValidatedRecord`** — typed output dataclass from the transformation layer

The per-row validation logic is **explicit Python conditionals** — not Pydantic validators. This was a deliberate revision after the initial AI-generated code used Pydantic for row validation, which made error code extraction unnecessarily complex.

**Cost:** Pydantic is listed as a core dependency but isn't the validation engine for per-row business rules.

---

## D-08 · SQLite fallback with zero friction, PostgreSQL for production

**Question**: How should the system handle the lack of Docker/PostgreSQL in a reviewer's local environment?

**Options considered:**

| Option | Reviewer experience |
|--------|---------------------|
| ❌ PostgreSQL-only | Reviewer must install Docker, start containers, wait — before they see a single line of output. |
| ❌ In-memory SQLite only | No persistent state. Can't demonstrate the quarantine table between runs. |
| ✅ **SQLite as default fallback, PostgreSQL as production path** | Clone → `pip install -r requirements.txt` → `python -m src.main ingest ...` → working in 60 seconds, no Docker. |

**Implementation:**
```python
# connection.py
is_sqlite = url.startswith("sqlite")
connect_args = {"check_same_thread": False} if is_sqlite else {}

# Pool settings only apply to PostgreSQL (SQLite doesn't support them)
if not is_sqlite:
    engine_kwargs["pool_size"] = settings.db.pool_size
    engine_kwargs["max_overflow"] = settings.db.max_overflow
```

The same codebase runs against both dialects. The upsert in `repositories.py` imports the dialect-specific `insert` from `sqlalchemy.dialects.postgresql` or `sqlalchemy.dialects.sqlite` based on the engine URL.

**Cost:** SQLite has known limitations at scale (write lock contention, no connection pooling). The 100K benchmark at 37.8s reflects SQLite sequential I/O, not a Python limitation. PostgreSQL would run the same test in ~12-15s.

---

## D-09 · `ON CONFLICT DO UPDATE` upsert for idempotency

**Question**: How should re-running the same input file be handled?

**Options considered:**

| Option | Behaviour |
|--------|-----------|
| ❌ `INSERT` only | Second run raises a `UNIQUE constraint violated` error. Pipeline crashes. |
| ❌ `DELETE + INSERT` | Loses `created_at` timestamp on re-run. Not atomic. |
| ❌ Check-then-insert | N+1 queries (1 SELECT + 1 INSERT per record). Slow at scale. Race condition in concurrent runs. |
| ✅ **`INSERT ... ON CONFLICT (customer_id) DO UPDATE SET ...`** | Single atomic statement. First run inserts. Second run updates. Same result. |

**Verified output:**
```
Run 1 (fresh DB):  Inserted: 920   Updated: 0
Run 2 (same file): Inserted: 0     Updated: 920
Run 3 (same file): Inserted: 0     Updated: 920   ← stable
```

**What fields are updated vs preserved:**
- Updated: `customer_name`, `email`, `phone`, `country_code`, `customer_type_id`, `registration_date`, `status`, `source_file`, `source_row_number`, `run_id`, `updated_at`
- Preserved: `customer_id` (PK), `created_at` (first-insert timestamp)

**Cost:** `updated_at` from the source record is used as the target `updated_at`, not the current DB timestamp. This preserves source timestamp semantics rather than the pipeline processing timestamp.

---

## D-10 · In-memory reference cache for FK lookups, not per-row queries

**Question**: How should referential integrity against `customer_types` be validated?

**Options considered:**

| Option | DB round-trips (N records) | Complexity |
|--------|---------------------------|-----------|
| ❌ `SELECT` per record | O(N) = 1000 queries for 1000 records | Simple but catastrophically slow |
| ❌ `JOIN` in a single batch query | O(1) query but requires restructuring the streaming architecture | Complex |
| ✅ **Pre-fetch valid IDs into `set[int]`, then O(1) membership test** | 1 query per run + N O(1) lookups | Simple and fast |

**Implementation:**
```python
# In PipelineRunner.run() — once per run:
active_types = customer_repo.get_active_customer_type_ids()
business_validator.set_valid_reference_keys(active_types)

# In BusinessValidator.validate_record() — per record:
if cust_type_id not in self.valid_customer_type_ids:  # O(1) set lookup
    errors.append(INVALID_REFERENCE)
```

**Cost:** The reference cache is loaded once at run start. If `customer_types` changes mid-run (a new type is added while the pipeline is processing), that run won't see the new type. This is acceptable — reference data changes rarely, and the next run would pick up the change.

---

## D-11 · `seen_ids: set[int]` for deduplication, not a dict or DB query

**Question**: How should intra-batch duplicates be detected?

**Options considered:**

| Option | Complexity | Memory | Accuracy |
|--------|-----------|--------|---------|
| ❌ `SELECT customer_id FROM customers` per record | O(N) DB queries | Low | Detects cross-run dupes too |
| ❌ `Dict[int, int]` to count occurrences | O(N) memory | Same as set, slightly more | Useful only if counting matters |
| ❌ Pandas `.duplicated()` on a full DataFrame | Requires full batch in memory | High for large files | Can't span chunks |
| ✅ **`seen_ids: set[int]`, persists across chunks within a run** | O(1) per lookup, O(unique_ids) memory | Low | Detects intra-file duplicates correctly |

**Initial AI-generated code used a `Dict[int, int]`**. Revised to `set[int]` during code review because counting how many times a duplicate appeared was not a business requirement, and a set is semantically cleaner.

**Cross-run duplicates** are handled by the upsert — not this validator. If `customer_id=42` appeared in yesterday's run and again today, it is not a duplicate — it's an update.

**Cost:** `seen_ids` grows with unique valid customer IDs in the file. For 100K records with ~92K valid ones, the set holds ~92K integers. At ~56 bytes each, that's ~5 MB — well within budget.

---

## D-12 · Duplicate count tracked separately from rejected count

**Question**: Should duplicate records be counted within the `rejected_count`?

**Options considered:**

| Option | Reconciliation equation |
|--------|------------------------|
| ❌ Duplicates counted inside `rejected_count` | `Input = Valid + Rejected` — simple, but conflates two different problem categories. |
| ✅ **Duplicates as a separate `duplicate_count` counter** | `Input = Valid + Rejected + Duplicates` — expressive and auditable. |

**Why this matters:**

`DUPLICATE_RECORD` and `MISSING_REQUIRED_FIELD` are fundamentally different problems:
- `MISSING_REQUIRED_FIELD` = upstream data quality issue, needs correction at source
- `DUPLICATE_RECORD` = ingestion design issue, could be resolved by deduplication at the source feed

Separating them allows operators to independently monitor and trend each category.

**Reconciliation equation (verified every run):**
```
1000 (input) = 920 (valid) + 60 (rejected) + 20 (duplicate) → PASS
```

**Code:**
```python
# runner.py — duplicate tracking is explicit and separate
tracker.record_duplicate(len(duplicate_cands))

# rejected does NOT include duplicates
non_dupe_errors = [e for e in error_codes if e != ErrorCodes.DUPLICATE_RECORD]
tracker.record_rejected(len(non_dupe_errors), non_dupe_errors)
```

---

## D-13 · Full raw JSON payload stored in quarantine — nothing stripped

**Question**: What data should be stored in `quarantine_records.raw_payload`?

**Options considered:**

| Option | Recovery capability |
|--------|---------------------|
| ❌ Just the error code | Operator can't see what was wrong with the value. Can't correct without re-fetching from source. |
| ❌ Store only the failing field value | Partial information. Replay needs the complete record to pass all validators. |
| ✅ **Full original row as JSON, all fields, raw string values** | Operator has everything needed to correct the record in-place and replay it without touching the source system. |

**Implementation:**
```python
# repositories.py
payload_str = json.dumps(c.raw_payload, default=str)
# default=str handles date objects, Decimals, etc. — nothing raises TypeError
```

The `raw_payload` is captured from the dict **before** any type conversions — it reflects exactly what arrived from the CSV parser.

**Cost:** Storage. Each quarantine record stores a complete row as JSON. For a row with 9 columns averaging 50 chars each, that's ~500 bytes per quarantine record. 10K quarantine records ≈ 5 MB. Acceptable for typical operational volumes.

---

## D-14 · UUID primary key for `quarantine_records`, integer for `customers`

**Question**: What type of primary key for each table?

| Table | Key type | Reason |
|-------|----------|--------|
| `customers` | `INTEGER` (`customer_id`) | Business-assigned stable identifier. Enables natural upsert on `ON CONFLICT (customer_id)`. |
| `quarantine_records` | `UUID` (string, `uuid4()`) | No natural business key. Each quarantine event is a unique occurrence. UUID prevents collisions across runs and tools without a DB sequence. |
| `pipeline_runs` | `VARCHAR(100)` (`RUN-{timestamp}-{hex}`) | Structured for human readability in logs and reports. |
| `replay_runs` | `UUID` (string, `uuid4()`) | Each replay attempt is an independent audit event. |

**Why not auto-increment integer for quarantine?**
Auto-increment IDs are fragile across database restores, migrations, and replicas. UUIDs generated in application code are portable, collision-resistant, and work consistently across both SQLite and PostgreSQL without schema changes.

---

## D-15 · Replay re-runs the full validation pipeline, not a shortcut path

**Question**: When replaying a corrected quarantine record, should the replay skip stages that the record previously passed?

**Options considered:**

| Option | Safety |
|--------|--------|
| ❌ Skip schema/required validation, run only business validation | A "corrected" payload might introduce a new structural error (e.g. operator sets customer_id to "ABC"). |
| ❌ Trust operator corrections, skip all validation | Direct write to target. Bypasses all data quality guarantees. |
| ✅ **Re-run the full pipeline: SchemaValidator → BusinessValidator → Transformer → Upsert** | Corrections are validated with the same rigour as original ingestion. |

**Implementation:** `ReplayService._process_single_replay()` calls `schema_validator.validate_record()`, then `business_validator.validate_record()`, then `transformer.transform_record()` — the same three classes, same methods, in the same order as `PipelineRunner.run()`.

The replay injects a new `run_id = "REPLAY-{timestamp}"` to maintain lineage distinguishing replayed records from original ingestions.

**Cost:** Replay is slower than a hypothetical skip-stage path. In practice, replay volumes are small (corrected quarantine records), so this is not a performance concern.

---

## D-16 · `retry_count` increments but never resets

**Question**: What should happen to `retry_count` when a `REPLAY_FAILED` record is corrected and replayed again?

**Options considered:**

| Option | Auditability |
|--------|-------------|
| ❌ Reset `retry_count` to 0 on each correction | Hides history. Looks like a fresh record on the third attempt. |
| ✅ **Increment `retry_count` on every `REPLAY_FAILED` outcome, never reset** | Provides a full history of how many correction attempts were made. |

**Code:**
```python
# replay_service.py
q_record.retry_count += 1  # always adds, never zeroes
```

**Initial AI-generated code reset `retry_count` to 0 on `CORRECTED` status.** This was identified during review as an audit trail bug. The revision increments it on each failure only.

**Operational use:** Operators can query `WHERE retry_count >= 3` to identify records that have been corrected and re-failed multiple times — these may require escalation rather than another correction attempt.

---

## D-17 · Shared SQLAlchemy Session across all repositories per run

**Question**: Should each repository create its own database connection?

**Options considered:**

| Option | Atomicity |
|--------|-----------|
| ❌ One session per repository | Quarantine writes and customer upserts are in separate transactions. If upsert commits but quarantine fails, state is inconsistent. |
| ❌ New session per batch chunk | Multiple commits per run. Partial commits on failure leave the database in a mid-run state. |
| ✅ **One session for the entire run, shared across all repositories** | All writes (quarantine + customers + pipeline_run update) commit atomically at run completion. If anything fails, the whole run rolls back. |

**Code:**
```python
# runner.py
with get_db_session(self.engine) as session:   # one session, one transaction
    customer_repo = CustomerRepository(session)
    quarantine_repo = QuarantineRepository(session)
    tracker = PipelineMetricsTracker(..., session=session)
    # all writes within this block share the same transaction
    # session.commit() called on context manager exit
    # session.rollback() called on any exception
```

**Cost:** A very large file with many chunks holds the transaction open for the entire run duration. For PostgreSQL, this means a long-running write transaction with potential lock contention. Mitigation: commit per-chunk for large files. Not implemented here — appropriate for the current scale.

---

## D-18 · Dataclass `ValidatedRecord` as persistence boundary type

**Question**: What type should cross the boundary between transformation and persistence?

**Options considered:**

| Option | Type safety |
|--------|-------------|
| ❌ Pass raw `Dict[str, Any]` to repository | Repository must defensively cast every field. Dict allows any value under any key. |
| ❌ Pydantic model | Overkill for a write-only internal boundary. Adds validation overhead. |
| ✅ **`@dataclass class ValidatedRecord`** | Typed fields, no extra overhead, immutable by convention, IDE-navigable. |

**Code:**
```python
@dataclass
class ValidatedRecord:
    customer_id: int            # guaranteed int — no "ABC" possible
    customer_name: str
    email: str
    phone: Optional[str]
    country_code: Optional[str]
    customer_type_id: int
    registration_date: date     # guaranteed date object — no string
    status: str
    updated_at: Optional[datetime]
    source_file: str
    source_row_number: int
    run_id: str
```

The repository receives a `List[ValidatedRecord]` and can directly access `.customer_id`, `.email`, etc. without any defensive casting. Type errors are caught at the transformation layer, not the persistence layer.

---

## D-19 · Lineage injected as `_`-prefixed internal keys on the record dict

**Question**: How should `source_file`, `source_row_number`, and `run_id` be carried through the pipeline without polluting the data payload?

**Options considered:**

| Option | Cleanliness |
|--------|-------------|
| ❌ Add them as top-level keys without prefix | They'd be treated as data columns by the schema validator. `validate_file_columns()` would flag them as `SCHEMA_UNEXPECTED_COLUMN`. |
| ❌ Carry them as a separate tuple/object alongside the dict | Every function signature changes. Increases parameter count across all stages. |
| ✅ **`_source_file`, `_source_row_number`, `_run_id` — underscore prefix convention** | The schema validator explicitly excludes `_`-prefixed keys from unexpected-column checks. All lineage travels with the record invisibly. |

**Code (CsvReader):**
```python
raw_dict["_source_file"] = source_file_name
raw_dict["_source_row_number"] = current_row_num
raw_dict["_run_id"] = run_id
```

**Code (SchemaValidator):**
```python
# When checking for unexpected columns:
unexpected = {c for c in unexpected if not c.startswith("_")}

# When building raw_payload for quarantine:
payload = {k: v for k, v in record.items() if not k.startswith("_")}
```

The lineage keys are stripped from `raw_payload` before JSON storage — they're pipeline internal metadata, not source data.

---

## D-20 · Structured `run_id` format over raw UUID

**Question**: What format should `run_id` use?

**Options considered:**

| Format | Example | Human readability |
|--------|---------|------------------|
| ❌ Raw UUID | `a3f7e1b2-9d4c-...` | Opaque. No temporal ordering. |
| ❌ Auto-increment integer | `1`, `2`, `3` | Simple but loses timestamp context. |
| ✅ **`RUN-{YYYYMMDD}-{HHMMSS}-{6-char hex}`** | `RUN-20261113-071105-4A5F5C` | Sortable by time. Human-readable in logs. Unique within the second (hex suffix). |

**Code:**
```python
def generate_run_id(self) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    short_id = uuid.uuid4().hex[:6].upper()
    return f"RUN-{ts}-{short_id}"
```

**Cost:** Not globally unique in the strict UUID sense — two runs starting within the same second could theoretically collide on the 6-hex suffix (~1 in 16M chance). Acceptable for a single-node pipeline. For multi-node distributed runs, switch to full UUID.

---

## D-21 · SQLAlchemy 2.0 ORM over raw SQL or SQLAlchemy Core

**Question**: How should database interaction be structured?

**Options considered:**

| Option | Pros | Cons |
|--------|------|------|
| Raw SQL (`cursor.execute`) | Maximum control, no abstraction overhead | No connection pooling management, no type safety, SQL injection risk, no dialect portability |
| SQLAlchemy Core (not ORM) | Full query builder, dialect-aware | No model definitions, requires manual column mapping |
| **✅ SQLAlchemy 2.0 ORM with `Mapped` types** | Type-annotated models, session lifecycle management, dialect-aware upserts, Pythonic relationship navigation | Slight learning curve for 2.0 `mapped_column` syntax vs 1.4 |

**Why SQLAlchemy 2.0 specifically?**
The 2.0 `Mapped[type] = mapped_column(...)` syntax provides compile-time type checking in IDEs, eliminating a class of `AttributeError` bugs common in 1.4-style ORM code.

```python
# SQLAlchemy 2.0 style (this project)
customer_id: Mapped[int] = mapped_column(Integer, primary_key=True)

# SQLAlchemy 1.4 style (avoided)
customer_id = Column(Integer, primary_key=True)
```

---

## D-22 · `PRAGMA foreign_keys=ON` for SQLite — not skipped

**Question**: Should SQLite foreign key enforcement be enabled?

**Default SQLite behaviour**: Foreign keys are **disabled by default**. A record with `customer_type_id=999` would insert successfully into `customers` even if `999` doesn't exist in `customer_types`.

**Decision**: Enable with an event hook:
```python
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
```

**Why**: The SQLite fallback must enforce the same referential integrity as PostgreSQL. Without this, a bug in the application-layer FK check would not be caught by the database — silent data corruption.

**Cost**: None. The pragma is applied once per connection.

---

## D-23 · Deterministic synthetic data via `Faker(seed=42)`, not real PII

**Question**: What data should be used for testing?

**Options considered:**

| Option | Reproducibility | Privacy | Completeness |
|--------|----------------|---------|-------------|
| ❌ Real customer data | ✅ Realistic | ❌ GDPR risk | ❌ Can't share in repo |
| ❌ Hardcoded static test fixtures | ✅ Reproducible | ✅ Safe | ❌ Limited coverage |
| ✅ **Faker with fixed `seed=42`** | ✅ Fully reproducible | ✅ No PII | ✅ Covers all error cases |

**What the generator produces (1000 rows, seed=42):**
- ~92% valid records with boundary conditions (min ID, max name length, earliest/latest dates)
- ~6% business rule violations (negative IDs, bad emails, future dates, invalid status, invalid FK)
- ~2% intra-file duplicates (same `customer_id` repeated)

Fixing `Faker.seed(seed)` and `random.seed(seed)` before generation means every reviewer gets **identical `customers.csv` and `expected_results.csv`** — no flaky test results from non-deterministic generation.

---

## D-24 · In-memory SQLite in tests, not mocking or monkey-patching

**Question**: How should database interactions be tested?

**Options considered:**

| Option | Fidelity | Speed | Reliability |
|--------|---------|-------|------------|
| ❌ Mock the session and repositories | Fast | No real SQL execution | Tests pass but bugs hide in SQL logic |
| ❌ Shared test PostgreSQL | Realistic | Slow (requires running PG) | Test isolation requires careful cleanup |
| ✅ **Fresh `sqlite:///:memory:` engine per test** | Real SQL execution | ~3.5s for 23 tests | No cleanup needed — engine is GC'd after test |

**Implementation:**
```python
# conftest.py
@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    init_db(eng)   # creates all tables + seeds reference data
    yield eng      # test runs here
    eng.dispose()  # connection closed, memory reclaimed
```

Each test gets its own isolated database. No shared state. No test ordering dependencies.

**Cost:** Tests exercise SQLite dialect, not PostgreSQL. The dialect-specific upsert paths (`pg_insert` vs `sqlite_insert`) are both exercised because the code branches on `dialect_name`.

---

## D-25 · Pure Python pipeline, no Kafka/Airflow/Spark

**Question**: Should the pipeline use distributed streaming or orchestration infrastructure?

**Requirement from brief**: "Complete tested vertical slice over excessive complexity."

**Options considered:**

| Technology | Use case fit | Local setup time |
|-----------|-------------|-----------------|
| Apache Kafka | Real-time streaming, >1M msg/s | 10+ min Docker setup, requires ZooKeeper or KRaft |
| Apache Airflow | DAG scheduling, multi-step dependency management | 20+ min Docker setup, 500MB+ images |
| Apache Spark | Distributed batch over multi-TB datasets | 30+ min setup, requires JVM |
| ✅ **Pure Python + Pandas + SQLAlchemy** | Batch CSV processing, 100K records in <40s | 60 seconds from clone to first run |

**Benchmarks against the brief:**

| Requirement | Verdict |
|-------------|---------|
| Handle 100K+ records | ✅ 100K in 37.8s, constant memory |
| Idempotent reruns | ✅ Verified with upsert |
| Quarantine + replay | ✅ Full state machine |
| Automated tests | ✅ 23 tests, 91% coverage |
| Runnable by reviewer in <5 min | ✅ No Docker required |

**When this decision should be revisited:**
- Record volume exceeds 10M rows/file → Spark or Dask
- Pipeline must run sub-second → Kafka consumer
- Multiple pipelines need dependency management → Airflow DAGs
- Multi-team parallel development → dedicated orchestration layer

For this assignment's scope, the complexity of distributed systems would obscure the data quality logic that is the actual subject of evaluation.

