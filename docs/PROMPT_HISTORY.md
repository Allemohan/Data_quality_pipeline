Below is a **single master prompt** you can paste into ChatGPT/Cursor/Claude to build the project step-by-step using the exact requirements you provided.

# MASTER PROMPT — DATA QUALITY QUARANTINE & REPLAY PIPELINE

You are a **Senior Data Engineer and Python Backend Engineer**. Build a complete, production-style **Data Quality, Quarantine, and Controlled Replay Pipeline** based on the requirements below.

The project must be suitable for a **0–3 years experience Data Engineering assignment**, but the implementation should demonstrate senior-level engineering practices: clean architecture, modular code, idempotency, observability, testing, reproducibility, and clear documentation.

---

## 1. BUSINESS PROBLEM

Build a data pipeline that:

1. Reads source records from CSV files.
2. Validates schema and data types.
3. Applies business/data-quality rules.
4. Separates valid and invalid records.
5. Loads valid records into PostgreSQL.
6. Stores invalid records in a PostgreSQL quarantine table.
7. Stores the exact error reason(s) for every rejected record.
8. Preserves source context so every rejected record can be traced back to:
   - source file
   - source row
   - run_id
   - ingestion timestamp
   - original payload
9. Provides a controlled replay mechanism for corrected quarantined records.
10. Ensures successful records are **not unnecessarily processed again** during replay.
11. Makes pipeline reruns safe and idempotent.
12. Records operational metrics and pipeline execution status.
13. Provides automated tests and reconciliation queries.
14. Supports both a small sample dataset and a configurable large dataset such as 100,000+ records.

---

# 2. TECHNOLOGY STACK

Use exactly this primary stack unless there is a strong technical reason otherwise:

### Programming
- Python 3.11+

### Input
- CSV

### Database
- PostgreSQL

### Data Processing
- Pandas

### Validation
- Pydantic

### Database Access
- SQLAlchemy
- psycopg

### Testing
- pytest
- pytest-cov

### Test Data
- Faker

### Configuration
- python-dotenv

### Version Control
- Git
- GitHub

Optional libraries may be added only when they provide clear value. Keep the project simple and maintainable.

---

# 3. PROJECT OBJECTIVES

The final project must demonstrate:

- ingestion
- schema validation
- data-quality validation
- transformation
- duplicate detection
- referential-integrity validation
- quarantine
- replay
- idempotency
- retry/recovery
- observability
- logging
- metrics
- testing
- reconciliation
- reproducible data generation
- performance measurement

Do not build unnecessary enterprise infrastructure.

Prefer a complete tested vertical slice over excessive complexity.

---

# 4. HIGH-LEVEL ARCHITECTURE

Implement the following flow:

CSV Source
    |
    v
File Ingestion
    |
    v
Schema Validation
    |
    v
Data Quality Validation
    |
    +----------------------+
    |                      |
    v                      v
Valid Records          Invalid Records
    |                      |
    v                      v
Transformation         Quarantine
    |                      |
    v                      |
PostgreSQL Target      Replay Process
                           |
                           v
                    Re-validation
                           |
                           v
                    Target PostgreSQL

Also maintain a separate pipeline execution/audit layer:

Pipeline Run
    |
    +-- run_id
    +-- source_file
    +-- start_time
    +-- end_time
    +-- status
    +-- input_count
    +-- valid_count
    +-- rejected_count
    +-- duplicate_count
    +-- error_count

---

# 5. DATABASE DESIGN

Design PostgreSQL tables for at least:

## 5.1 Target table

Example:

`customers`

Include:

- customer_id
- customer_name
- email
- phone
- date_of_birth
- registration_date
- status
- source_file
- source_row_number
- run_id
- created_at
- updated_at

Use appropriate PostgreSQL data types.

---

## 5.2 Quarantine table

Create:

`quarantine_records`

It must contain:

- quarantine_id
- source_record_id
- source_file
- source_row_number
- run_id
- raw_payload
- error_code
- error_message
- validation_stage
- status
- retry_count
- first_seen_at
- last_seen_at
- corrected_at
- replayed_at

Possible statuses:

- QUARANTINED
- CORRECTED
- REPLAYED
- REPLAY_FAILED
- RESOLVED

Do not silently delete quarantine history.

---

## 5.3 Pipeline runs table

Create:

`pipeline_runs`

Fields:

- run_id
- source_file
- started_at
- completed_at
- status
- input_count
- valid_count
- rejected_count
- duplicate_count
- processing_time_seconds
- error_message

Possible status values:

- STARTED
- SUCCESS
- PARTIAL_SUCCESS
- FAILED

---

## 5.4 Reference table

Create at least one reference table so that referential integrity can be demonstrated.

Example:

`customer_types`

or

`countries`

or

`products`

The source data should contain a reference key that must exist in this table.

---

## 5.5 Replay audit table

Create:

`replay_runs`

Track:

- replay_run_id
- quarantine_id
- original_run_id
- replay_started_at
- replay_completed_at
- status
- old_error
- new_error
- target_record_id

---

# 6. ENTITY AND RELATIONSHIP DESIGN

Document:

- entities
- primary keys
- foreign keys
- relationships
- record grain
- nullable fields
- unique fields
- required fields
- derived fields
- business rules

Use stable deterministic identifiers.

Use:

- snake_case table names
- snake_case column names
- ISO-8601 timestamps

Every pipeline execution must have a unique `run_id`.

Every input file must be traceable through `source_file`.

---

# 7. INPUT CSV SCHEMA

Define a clear CSV schema.

Example:

```text
customer_id
customer_name
email
phone
country_code
customer_type_id
registration_date
status
updated_at
```

Document every column in a data dictionary.

For each field specify:

- datatype
- nullable/not nullable
- unique/not unique
- validation rules
- business meaning
- transformation rules

---

# 8. VALIDATION RULES

Implement multiple validation layers.

## Schema validation

Detect:

- missing columns
- unexpected columns
- incorrect datatype
- malformed CSV
- invalid date format
- invalid numeric values

---

## Required-field validation

Reject records where required fields are missing.

Example:

```text
customer_id cannot be null
customer_name cannot be null
email cannot be null
customer_type_id cannot be null
```

---

## Business validation

Implement rules such as:

```text
customer_id > 0

email must have valid email format

status must be one of:
ACTIVE
INACTIVE
SUSPENDED

registration_date cannot be in the future

customer_type_id must exist in reference table
```

---

## Duplicate validation

Detect:

- duplicate customer_id
- duplicate event/record
- duplicate rows inside the same file
- duplicate records across reruns

Clearly distinguish:

```text
duplicate input
```

from:

```text
already successfully processed record
```

---

# 9. QUARANTINE DESIGN

Every invalid record must be stored in the quarantine table.

Do not simply print errors and discard records.

For example:

```text
source_record_id: CUST-1004

error_code:
INVALID_EMAIL

error_message:
Email address is not valid

source_file:
customers_001.csv

source_row_number:
14

run_id:
RUN-20261112-001

raw_payload:
{...original record...}
```

A single record may have multiple validation errors.

Design the system so error reasons are not lost.

---

# 10. REPLAY PROCESS

Implement a controlled replay mechanism.

The replay process must:

1. Select records from quarantine.
2. Allow only eligible records to be replayed.
3. Revalidate corrected records.
4. Apply transformations again.
5. Load successful records into the target.
6. Update quarantine status.
7. Record replay audit information.
8. Prevent successful records from being loaded twice.
9. Increment retry count on failed replay.
10. Preserve the original error history.

Example:

```text
QUARANTINED
     |
     v
CORRECTED
     |
     v
REPLAY
     |
     +---- valid ----> TARGET
     |
     +---- invalid --> REPLAY_FAILED
```

Replay must be controlled and auditable.

---

# 11. IDEMPENCY

The pipeline must be idempotent.

If this command is executed twice:

```bash
python -m src.pipeline --input data/sample/customers.csv
```

the second execution must not create unintended duplicate target records.

Use deterministic keys and database constraints.

Document:

- retry behavior
- restart behavior
- backfill behavior
- duplicate handling
- partial failure handling

---

# 12. RUN TRACKING

Every execution must generate a `run_id`.

Example:

```text
RUN-20261112-001
```

Record:

```text
run_id
source_file
start_time
end_time
input_count
valid_count
rejected_count
duplicate_count
status
processing_time
```

Example log:

```text
Pipeline started
run_id=RUN-20261112-001

Input records: 1000
Valid records: 920
Rejected records: 60
Duplicates: 20

Pipeline completed successfully
Processing time: 2.31 seconds
```

---

# 13. LOGGING

Use Python's standard `logging` module.

Do not use random print statements for operational logging.

Logs should contain useful context such as:

```text
run_id
source_file
record_id
validation_stage
error_code
```

---

# 14. DATA GENERATOR

Build a configurable synthetic data generator.

Example command:

```bash
python -m src.generator --rows 1000 --seed 42
```

Large dataset:

```bash
python -m src.generator --rows 100000 --seed 42
```

The generator must use a fixed random seed.

Generate:

### Valid records

Normal records with valid values.

### Boundary values

Examples:

- minimum valid ID
- maximum reasonable string length
- earliest valid date
- latest valid date

### Missing values

Examples:

```text
email = NULL
customer_name = NULL
customer_type_id = NULL
```

### Duplicate records

Generate deliberate duplicates.

### Invalid records

Generate deliberately labelled invalid cases.

Examples:

```text
invalid_email
missing_customer_id
invalid_status
future_registration_date
invalid_reference_key
malformed_date
```

### Late/out-of-order records

Generate records where:

```text
updated_at
```

does not follow input order.

The generator must be deterministic when using the same seed.

---

# 15. IMPORTANT DATA GENERATION RULE

Expected-answer labels must not be included in the production input schema.

If you need expected results for testing, keep them separately.

For example:

```text
data/
    input/
        customers.csv

    expected/
        expected_results.csv
```

Do not leak test labels into the actual pipeline input.

---

# 16. PROJECT STRUCTURE

Create this project structure:

```text
data-quality-pipeline/
│
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── docker-compose.yml
├── pytest.ini
│
├── config/
│   └── config.yaml
│
├── data/
│   ├── input/
│   ├── expected/
│   ├── output/
│   └── quarantine/
│
├── sql/
│   ├── schema.sql
│   ├── seed.sql
│   └── reconciliation.sql
│
├── src/
│   ├── __init__.py
│   │
│   ├── config/
│   │   └── settings.py
│   │
│   ├── ingestion/
│   │   └── csv_reader.py
│   │
│   ├── validation/
│   │   ├── schema_validator.py
│   │   ├── business_validator.py
│   │   └── duplicate_validator.py
│   │
│   ├── transformation/
│   │   └── transformer.py
│   │
│   ├── database/
│   │   ├── connection.py
│   │   ├── models.py
│   │   └── repositories.py
│   │
│   ├── quarantine/
│   │   └── quarantine_service.py
│   │
│   ├── replay/
│   │   └── replay_service.py
│   │
│   ├── monitoring/
│   │   └── metrics.py
│   │
│   ├── pipeline/
│   │   └── runner.py
│   │
│   └── main.py
│
├── generators/
│   └── generate_data.py
│
└── tests/
    ├── test_ingestion.py
    ├── test_validation.py
    ├── test_duplicates.py
    ├── test_quarantine.py
    ├── test_replay.py
    ├── test_idempotency.py
    └── test_pipeline.py
```

---

# 17. DATABASE CONNECTION

Use environment variables.

Example `.env.example`:

```env
DATABASE_URL=postgresql+psycopg://postgres:password@localhost:5432/data_pipeline
```

Never hard-code passwords.

Never commit `.env`.

---

# 18. DOCKER

Provide a simple PostgreSQL Docker setup.

Example:

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: data_pipeline
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
```

The README must explain how to start PostgreSQL.

---

# 19. PIPELINE COMMANDS

Provide simple CLI commands.

Example:

```bash
python -m src.main ingest --input data/input/customers.csv
```

Generate sample data:

```bash
python -m generators.generate_data --rows 1000 --seed 42
```

Generate large data:

```bash
python -m generators.generate_data --rows 100000 --seed 42
```

Run replay:

```bash
python -m src.main replay
```

Run tests:

```bash
pytest
```

Run coverage:

```bash
pytest --cov=src --cov-report=term-missing
```

---

# 20. AUTOMATED TESTS

Implement pytest tests for at least:

### Happy path

Valid records successfully reach PostgreSQL.

### Empty input

Pipeline handles empty CSV without crashing.

### Malformed records

Malformed records go to quarantine.

### Missing required fields

Records are rejected with clear error codes.

### Duplicate records

Duplicates are detected correctly.

### Null values

Required nulls are rejected.

### Invalid reference key

Foreign-key/reference validation works.

### Invalid email

Correct validation error is generated.

### Future registration date

Correct business-rule error is generated.

### Rerun

Running the same input twice does not create duplicate target records.

### Replay

Corrected quarantined record can be replayed successfully.

### Replay failure

Incorrectly corrected record remains rejected.

### Partial failure

Pipeline records the failure correctly.

---

# 21. TEST COVERAGE

Target meaningful test coverage.

Run:

```bash
pytest --cov=src --cov-report=term-missing
```

Include the coverage result in the final documentation.

Do not artificially inflate coverage with meaningless tests.

---

# 22. RECONCILIATION

Create SQL queries that verify:

```text
input_count
=
valid_count + rejected_count + duplicate_count
```

where applicable.

Also verify:

```text
valid_count
=
target_inserted_count
```

taking into account idempotent reruns.

Provide aggregate checks such as:

```sql
SELECT COUNT(*)
FROM customers;
```

and:

```sql
SELECT status, COUNT(*)
FROM quarantine_records
GROUP BY status;
```

Also provide checks for:

- duplicate target IDs
- orphan reference keys
- rejected record counts
- replay success/failure
- source-to-target reconciliation

---

# 23. SAMPLE OUTPUT

Create a sample run report similar to:

```text
=================================================
PIPELINE RUN REPORT
=================================================

Run ID              : RUN-20261112-001
Source File         : customers.csv

Input Records       : 1000
Valid Records       : 920
Rejected Records    : 60
Duplicate Records   : 20
Target Records      : 920

Processing Time     : 2.31 seconds

Status              : SUCCESS

=================================================
QUARANTINE SUMMARY
=================================================

INVALID_EMAIL       : 15
MISSING_EMAIL       : 10
INVALID_STATUS      : 12
INVALID_REFERENCE   : 13
FUTURE_DATE         : 10

=================================================
RECONCILIATION
=================================================

Input = Valid + Rejected + Duplicate

1000 = 920 + 60 + 20

PASS
```

---

# 24. PERFORMANCE TEST

Measure processing time for:

```text
1,000 records
10,000 records
100,000 records
```

Record:

```text
record_count
processing_time
records_per_second
```

Example:

```text
1,000       -> 0.8 sec
10,000      -> 4.2 sec
100,000     -> 38.5 sec
```

Do not invent these numbers. Generate actual measurements from the implementation.

Identify at least one reasonable optimization.

For example:

- batch database inserts
- vectorized Pandas operations
- database indexes
- bulk inserts
- reduced database round trips

Explain why the optimization helps.

---

# 25. EDGE CASE MATRIX

Create a test/validation matrix:

| Scenario | Expected Result |
|---|---|
| Valid record | Target |
| Missing required field | Quarantine |
| Invalid email | Quarantine |
| Invalid status | Quarantine |
| Invalid reference | Quarantine |
| Duplicate input | Duplicate handling |
| Duplicate rerun | No unintended duplicate |
| Empty CSV | Successful empty run |
| Malformed CSV | Pipeline failure with clear error |
| Unexpected column | Schema failure |
| Missing column | Schema failure |
| Late record | Accepted according to defined rule |
| Out-of-order record | Accepted according to defined rule |
| Corrected quarantine record | Replay |
| Incorrect correction | Replay failure |
| Partial database failure | Safe retry |

---

# 26. ERROR CODES

Use standardized error codes.

Examples:

```text
SCHEMA_MISSING_COLUMN
SCHEMA_UNEXPECTED_COLUMN
INVALID_DATA_TYPE
MISSING_REQUIRED_FIELD
INVALID_EMAIL
INVALID_STATUS
INVALID_REFERENCE
FUTURE_DATE
DUPLICATE_RECORD
MALFORMED_RECORD
TRANSFORMATION_ERROR
DATABASE_ERROR
REPLAY_ERROR
```

Do not rely only on free-form error messages.

---

# 27. DATA LINEAGE

Every target/quarantine record must be traceable back to:

```text
source_file
source_row_number
run_id
source_record_id
```

This should allow an operator to answer:

> Where did this record come from?

and:

> Why was this record rejected?

---

# 28. LATE-ARRIVING DATA

Define how late-arriving data is handled.

Do not silently discard it.

Document whether the pipeline:

- accepts it
- updates existing records
- stores historical versions
- rejects it

Choose a simple strategy appropriate for this project and explain the trade-off.

---

# 29. HISTORY

If the source contains changes over time, do not silently overwrite important history.

Implement a simple history strategy if appropriate.

For example:

```text
customer_id
effective_from
effective_to
is_current
```

If full SCD Type 2 is unnecessary, document why.

---

# 30. README

Create a professional README containing:

## Project Overview

## Business Problem

## Objectives

## Architecture

## Technology Stack

## Project Structure

## Database Schema

## Data Dictionary

## Validation Rules

## Quarantine Design

## Replay Design

## Idempotency Strategy

## Error Handling

## Logging and Monitoring

## Setup Instructions

## Environment Variables

## Database Setup

## Generate Sample Data

## Run Pipeline

## Run Replay

## Run Tests

## Test Coverage

## Performance Results

## Reconciliation Results

## Assumptions

## Known Limitations

## Future Improvements

## Design Trade-offs

---

# 31. ARCHITECTURE DIAGRAM

Include a Mermaid diagram in README.

Example:

```mermaid
flowchart LR

A[CSV Source] --> B[Ingestion]

B --> C[Schema Validation]

C --> D[Business Validation]

D --> E{Valid?}

E -->|Yes| F[Transformation]

F --> G[PostgreSQL Target]

E -->|No| H[Quarantine Store]

H --> I[Correction]

I --> J[Controlled Replay]

J --> D

G --> K[Reconciliation]

H --> K

B --> L[Pipeline Run Metrics]
D --> L
G --> L
H --> L
J --> L
```

---

# 32. DESIGN TRADE-OFFS

Write a short design note explaining decisions such as:

### Why Pandas?

Simple and appropriate for the target scale.

### Why PostgreSQL?

Reliable relational storage with constraints, indexes, and transactional guarantees.

### Why Pydantic?

Strong schema/type validation.

### Why quarantine instead of dropping invalid data?

Preserves traceability and allows controlled recovery.

### Why idempotency?

Prevents duplicate target records during retries and reruns.

### Why synthetic data?

Avoids privacy and licensing issues.

### Why not Kafka/Airflow/Spark?

The assignment prioritizes a complete local vertical slice within 72 hours. Those tools can be future extensions if scale requires them.

---

# 33. SECURITY

Do not:

- commit passwords
- commit API keys
- commit `.env`
- use private production data
- expose credentials in logs

Use:

```text
.env
.env.example
.gitignore
```

---

# 34. CODE QUALITY

Follow:

- PEP 8
- type hints
- docstrings for important functions
- meaningful variable names
- small reusable functions
- separation of concerns
- configuration outside business logic
- proper exception handling

Avoid:

- giant functions
- hard-coded database credentials
- duplicated logic
- unnecessary frameworks
- unexplained magic numbers

---

# 35. DEVELOPMENT ORDER

Build the project in this order:

### Phase 1
Requirements and architecture.

### Phase 2
Project setup.

### Phase 3
PostgreSQL setup.

### Phase 4
Database schema.

### Phase 5
Synthetic data generator.

### Phase 6
CSV ingestion.

### Phase 7
Pydantic schema validation.

### Phase 8
Business validation.

### Phase 9
Duplicate detection.

### Phase 10
Transformation.

### Phase 11
Target loading.

### Phase 12
Quarantine implementation.

### Phase 13
Pipeline run tracking.

### Phase 14
Replay mechanism.

### Phase 15
Idempotency.

### Phase 16
Logging and metrics.

### Phase 17
Automated tests.

### Phase 18
Reconciliation queries.

### Phase 19
Performance testing.

### Phase 20
README and final documentation.

---

# 36. IMPORTANT IMPLEMENTATION RULE

Do NOT generate the entire project blindly in one response.

Build it incrementally.

For each phase:

1. Explain the objective.
2. Show the files being created/modified.
3. Provide complete code for those files.
4. Explain how the code works.
5. Give exact commands to run.
6. Give expected output.
7. Identify common errors.
8. Provide a validation/checklist.
9. Only move to the next phase after the current phase is logically complete.

Do not skip requirements from the original problem statement.

---

# 37. FINAL DELIVERABLE CHECKLIST

Before declaring the project complete, verify:

```text
[ ] Python 3.11+
[ ] PostgreSQL
[ ] CSV ingestion
[ ] Pandas processing
[ ] Pydantic validation
[ ] SQLAlchemy
[ ] psycopg
[ ] Faker generator
[ ] pytest
[ ] pytest-cov
[ ] dotenv configuration
[ ] schema validation
[ ] business validation
[ ] duplicate handling
[ ] referential integrity
[ ] quarantine
[ ] replay
[ ] idempotency
[ ] retry handling
[ ] run tracking
[ ] logging
[ ] metrics
[ ] reconciliation
[ ] sample dataset
[ ] 100,000+ configurable generator
[ ] fixed random seed
[ ] malformed input handling
[ ] empty batch handling
[ ] null handling
[ ] duplicate handling
[ ] late/out-of-order handling
[ ] unexpected schema handling
[ ] invalid reference handling
[ ] automated tests
[ ] coverage report
[ ] performance measurement
[ ] architecture diagram
[ ] data dictionary
[ ] README
[ ] design/trade-off note
[ ] .env.example
[ ] .gitignore
[ ] requirements.txt
[ ] Docker PostgreSQL setup
```

---

# 38. FINAL RESPONSE FORMAT

At the end of implementation, provide:

1. Architecture overview
2. Project structure
3. Database schema
4. Validation rules
5. Quarantine workflow
6. Replay workflow
7. Idempotency strategy
8. Test results
9. Coverage results
10. Performance results
11. Reconciliation results
12. Sample pipeline output
13. Known limitations
14. Future improvements
15. GitHub submission checklist

The final implementation must be runnable from a clean environment by following the README.

Prioritize **correctness, data quality, traceability, testing, idempotency, recovery, observability, maintainability, and reproducibility** over unnecessary complexity.

**Best way to use this:** give this prompt to your coding AI and tell it to start with **Phase 1 only**. Then implement and test each phase sequentially rather than asking it to generate all files at once.