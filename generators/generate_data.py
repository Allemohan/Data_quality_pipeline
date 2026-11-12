"""Synthetic test data generator for Data Quality Quarantine & Replay Pipeline.

Generates deterministic CSV datasets with valid records, boundary values,
deliberate duplicates, missing fields, and business rule violations.
Adheres strictly to the rule: NO expected labels are leaked into input CSV.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from faker import Faker

VALID_STATUSES = ["ACTIVE", "INACTIVE", "SUSPENDED"]
VALID_CUSTOMER_TYPE_IDS = [1, 2, 3, 4, 5]
VALID_COUNTRY_CODES = ["US", "GB", "DE", "FR", "IN", "CA", "AU", "JP"]


def generate_dataset(
    rows: int = 1000,
    seed: int = 42,
    output_path: Optional[Path | str] = None,
    expected_path: Optional[Path | str] = None,
) -> Tuple[int, int, int]:
    """Generates synthetic dataset and optional expected ground truth file.

    Returns:
        (total_rows, expected_valid_count, expected_invalid_or_duplicate_count)
    """
    random.seed(seed)
    Faker.seed(seed)
    fake = Faker()

    base_date = date.today()
    start_date = base_date - timedelta(days=365 * 3)

    records: List[Dict[str, Any]] = []
    expected_labels: List[Dict[str, Any]] = []

    # Calculate proportions
    duplicate_target = max(1, int(rows * 0.02))  # ~2% duplicates
    invalid_target = max(1, int(rows * 0.06))    # ~6% invalid records
    valid_target = rows - duplicate_target - invalid_target

    current_id = 1
    valid_ids: List[int] = []

    # 1. Generate Valid Records (including boundary conditions)
    for i in range(valid_target):
        cust_id = current_id
        current_id += 1
        valid_ids.append(cust_id)

        # Boundary checks
        if i == 0:
            # Minimum valid ID, today's date
            reg_date = base_date
            cust_name = "A"
        elif i == 1:
            # Earliest boundary date
            reg_date = start_date
            cust_name = "Maximilian Bartholomew Alexander Von Montgomery"
        else:
            delta_days = random.randint(0, (base_date - start_date).days)
            reg_date = start_date + timedelta(days=delta_days)
            cust_name = fake.name()

        email = f"user_{cust_id}_{fake.user_name()}@{fake.free_email_domain()}"
        phone = fake.phone_number() if random.random() > 0.1 else None
        country = random.choice(VALID_COUNTRY_CODES)
        cust_type = random.choice(VALID_CUSTOMER_TYPE_IDS)
        status = random.choice(VALID_STATUSES)

        # Generate jittered/out-of-order updated_at
        jitter_hours = random.randint(-48, 48)
        updated_dt = datetime.combine(reg_date, datetime.min.time()) + timedelta(
            days=random.randint(0, 30), hours=jitter_hours
        )
        updated_at_str = updated_dt.strftime("%Y-%m-%d %H:%M:%S")

        rec = {
            "customer_id": cust_id,
            "customer_name": cust_name,
            "email": email,
            "phone": phone or "",
            "country_code": country,
            "customer_type_id": cust_type,
            "registration_date": reg_date.strftime("%Y-%m-%d"),
            "status": status,
            "updated_at": updated_at_str,
        }
        records.append(rec)
        expected_labels.append({
            "source_row": len(records),
            "customer_id": cust_id,
            "is_valid": True,
            "expected_category": "VALID",
            "expected_error": "",
        })

    # 2. Generate Deliberate Duplicates
    for _ in range(duplicate_target):
        if not valid_ids:
            break
        dupe_source_id = random.choice(valid_ids)
        # Duplicate record with slight variation in non-key fields
        dupe_rec = {
            "customer_id": dupe_source_id,
            "customer_name": fake.name(),
            "email": f"duplicate_{dupe_source_id}@{fake.free_email_domain()}",
            "phone": fake.phone_number(),
            "country_code": "US",
            "customer_type_id": 1,
            "registration_date": base_date.strftime("%Y-%m-%d"),
            "status": "ACTIVE",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        records.append(dupe_rec)
        expected_labels.append({
            "source_row": len(records),
            "customer_id": dupe_source_id,
            "is_valid": False,
            "expected_category": "DUPLICATE",
            "expected_error": "DUPLICATE_RECORD",
        })

    # 3. Generate Invalid Records (Business & Schema Rule Violations)
    invalid_cases = [
        "MISSING_REQUIRED_NAME",
        "MISSING_REQUIRED_EMAIL",
        "MISSING_REQUIRED_TYPE",
        "INVALID_EMAIL_FORMAT",
        "INVALID_STATUS",
        "FUTURE_REGISTRATION_DATE",
        "INVALID_REFERENCE_KEY",
        "MALFORMED_DATE",
        "INVALID_CUSTOMER_ID",
    ]

    for i in range(invalid_target):
        case_type = invalid_cases[i % len(invalid_cases)]
        cust_id = current_id
        current_id += 1

        cust_name = fake.name()
        email = fake.email()
        cust_type: Any = random.choice(VALID_CUSTOMER_TYPE_IDS)
        reg_date_str = base_date.strftime("%Y-%m-%d")
        status = "ACTIVE"
        expected_error = ""

        if case_type == "MISSING_REQUIRED_NAME":
            cust_name = ""
            expected_error = "MISSING_REQUIRED_FIELD"
        elif case_type == "MISSING_REQUIRED_EMAIL":
            email = ""
            expected_error = "MISSING_REQUIRED_FIELD"
        elif case_type == "MISSING_REQUIRED_TYPE":
            cust_type = ""
            expected_error = "MISSING_REQUIRED_FIELD"
        elif case_type == "INVALID_EMAIL_FORMAT":
            email = "invalid_user_at_domain_com"
            expected_error = "INVALID_EMAIL"
        elif case_type == "INVALID_STATUS":
            status = "PENDING_APPROVAL"  # Not in allowed statuses
            expected_error = "INVALID_STATUS"
        elif case_type == "FUTURE_REGISTRATION_DATE":
            future_date = base_date + timedelta(days=random.randint(10, 180))
            reg_date_str = future_date.strftime("%Y-%m-%d")
            expected_error = "FUTURE_DATE"
        elif case_type == "INVALID_REFERENCE_KEY":
            cust_type = 999  # Key not present in customer_types table
            expected_error = "INVALID_REFERENCE"
        elif case_type == "MALFORMED_DATE":
            reg_date_str = "not-a-valid-date"
            expected_error = "MALFORMED_RECORD"
        elif case_type == "INVALID_CUSTOMER_ID":
            cust_id = -abs(cust_id)
            expected_error = "INVALID_CUSTOMER_ID"

        rec = {
            "customer_id": cust_id,
            "customer_name": cust_name,
            "email": email,
            "phone": fake.phone_number(),
            "country_code": "US",
            "customer_type_id": cust_type,
            "registration_date": reg_date_str,
            "status": status,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        records.append(rec)
        expected_labels.append({
            "source_row": len(records),
            "customer_id": cust_id,
            "is_valid": False,
            "expected_category": "INVALID",
            "expected_error": expected_error,
        })

    # Shuffle to simulate realistic ingestion order
    combined = list(zip(records, expected_labels))
    random.shuffle(combined)
    records, expected_labels = zip(*combined)

    # Re-assign row numbers in expected labels after shuffle
    for idx, el in enumerate(expected_labels, start=2):  # row 1 is header
        el["source_row"] = idx

    # Write input CSV (Notice: Strictly NO expected result labels!)
    fieldnames = [
        "customer_id",
        "customer_name",
        "email",
        "phone",
        "country_code",
        "customer_type_id",
        "registration_date",
        "status",
        "updated_at",
    ]

    out_file = Path(output_path) if output_path else Path("data/input/customers.csv")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    # Write expected labels file if requested
    if expected_path:
        exp_file = Path(expected_path)
        exp_file.parent.mkdir(parents=True, exist_ok=True)
        exp_fields = ["source_row", "customer_id", "is_valid", "expected_category", "expected_error"]
        with open(exp_file, "w", newline="", encoding="utf-8") as f:
            exp_writer = csv.DictWriter(f, fieldnames=exp_fields)
            exp_writer.writeheader()
            exp_writer.writerows(expected_labels)

    return len(records), valid_target, (duplicate_target + invalid_target)


def main():
    parser = argparse.ArgumentParser(description="Synthetic Data Generator for Data Quality Pipeline")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate (default: 1000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--output", type=str, default="data/input/customers.csv", help="Destination CSV path")
    parser.add_argument("--expected", type=str, default="data/expected/expected_results.csv", help="Expected labels destination")

    args = parser.parse_args()
    total, valid, rejected = generate_dataset(
        rows=args.rows,
        seed=args.seed,
        output_path=args.output,
        expected_path=args.expected,
    )
    print("=" * 60)
    print("DATA GENERATION COMPLETE")
    print("=" * 60)
    print(f"Total Rows Generated : {total}")
    print(f"Expected Valid       : {valid}")
    print(f"Expected Invalid/Dupe: {rejected}")
    print(f"Output File          : {args.output}")
    print(f"Expected Labels File : {args.expected}")
    print(f"Random Seed          : {args.seed}")
    print("=" * 60)


if __name__ == "__main__":
    main()

