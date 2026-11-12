# Data Dictionary & Entity Relationship Specification

## 1. Domain Overview
The **Cloud Financial Payment Transactions Lake** stores high-velocity transaction streams ingested from point-of-sale terminals, payment gateways, and e-commerce checkouts. The data lake is partitioned by event date (`event_date=YYYY-MM-DD`) and optimized for analytical querying, compliance auditing, and daily financial reconciliation.

---

## 2. Entities & Schema Definitions

### 2.1 Reference Dimension: `merchants`
* **Grain**: One row per enrolled business merchant.
* **Storage Location**: `data/reference/merchants.json`
* **Integrity**: Master dimension used for foreign key referential integrity validation.

| Column Name | Data Type | Nullable | Primary Key | Description & Permitted Values |
| :--- | :--- | :--- | :--- | :--- |
| `merchant_id` | STRING | NO | YES | Unique deterministic identifier (e.g. `MERC_0001`). Format `^MERC_[0-9]{4}$`. |
| `merchant_name` | STRING | NO | NO | Legal business entity name. |
| `country_code` | STRING | NO | NO | ISO 3166-1 alpha-2 or alpha-3 country code (e.g. `US`, `GB`, `DE`). |
| `status` | STRING | NO | NO | Account status: `ACTIVE`, `SUSPENDED`, `INACTIVE`. Only `ACTIVE` merchants can process transactions. |
| `created_at` | STRING | NO | NO | ISO-8601 UTC timestamp of merchant enrollment. |

---

### 2.2 Target Fact Entity: `transactions` (Silver Lake)
* **Grain**: One row per individual financial transaction event.
* **Storage Location**: `data/lake/silver/partitioned_compacted/event_date=YYYY-MM-DD/compacted_part_*.parquet`
* **Partition Key**: `event_date` (Hive-style directory partition: `event_date=YYYY-MM-DD`)
* **Sort Order**: `(merchant_id, transaction_timestamp)` within each physical Parquet file.

| Column Name | Physical Type | Nullable | Primary Key | Foreign Key | Description & Business Rules |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `transaction_id` | STRING (`pa.string`) | NO | YES | - | Deterministic unique transaction identifier. Must be non-empty and unique lake-wide. |
| `merchant_id` | STRING (`pa.string`) | NO | NO | `merchants.merchant_id` | Enrolled merchant identifier. Foreign key must exist in `merchants` with status `ACTIVE`. |
| `customer_id` | STRING (`pa.string`) | NO | NO | - | Customer identifier (e.g. `CUST_10423`). |
| `amount` | FLOAT64 (`pa.float64`) | NO | NO | - | Transaction financial amount. Must be strictly positive (`amount > 0.00`). |
| `currency` | STRING (`pa.string`) | NO | NO | - | ISO-4217 3-letter currency code: `USD`, `EUR`, `GBP`, `CAD`, `AUD`, `JPY`. |
| `status` | STRING (`pa.string`) | NO | NO | - | Transaction settlement state: `COMPLETED`, `PENDING`, `FAILED`, `REFUNDED`. |
| `payment_method` | STRING (`pa.string`) | NO | NO | - | Channel: `CREDIT_CARD`, `DEBIT_CARD`, `WIRE_TRANSFER`, `ACH`, `DIGITAL_WALLET`. |
| `transaction_timestamp` | STRING (`pa.string`) | NO | NO | - | Event creation time in ISO-8601 UTC (e.g. `2026-11-10T14:30:00Z`). |
| `event_date` | STRING (`pa.string`) | NO | PARTITION | - | **Partition column** (`YYYY-MM-DD`) derived deterministically from `transaction_timestamp`. |
| `event_year` | INT32 (`pa.int32`) | NO | NO | - | Derived calendar year (`YYYY`). |
| `event_month` | INT32 (`pa.int32`) | NO | NO | - | Derived calendar month (`1-12`). |
| `run_id` | STRING (`pa.string`) | NO | NO | - | UUID of the pipeline execution run that ingested this record (lineage). |
| `source_file` | STRING (`pa.string`) | NO | NO | - | Filename of the source batch file where this event originated (lineage). |
| `ingested_at` | STRING (`pa.string`) | NO | NO | - | ISO-8601 UTC timestamp of ingestion into the data lake. |

---

### 2.3 Quarantine Entity: `quarantine`
* **Grain**: One row per rejected raw record.
* **Storage Location**: `data/lake/quarantine/reject_date=YYYY-MM-DD/quarantine_*.parquet` and `.json`
* **Purpose**: Full operational traceability for malformed, corrupt, or policy-violating events.

| Column Name | Physical Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `raw_record` | STRING | NO | Full unmodified string/JSON representation of the incoming record. |
| `rejection_reason` | STRING | NO | High-level classification enum (see below). |
| `error_details` | STRING | NO | Granular diagnostic error message or exception traceback. |
| `run_id` | STRING | NO | UUID of the pipeline run that rejected the record. |
| `source_file` | STRING | NO | Source file name. |
| `rejected_at` | STRING | NO | ISO-8601 timestamp of quarantine action. |

#### Rejection Classification Enum:
* `MALFORMED_RECORD`: Unparseable JSON or corrupted stream payload.
* `MISSING_REQUIRED_FIELD`: Null or missing mandatory attributes (e.g. missing `transaction_id` or `amount`).
* `INVALID_AMOUNT`: Non-numeric, zero, or negative transaction amounts (`amount <= 0.0`).
* `INVALID_CURRENCY`: Currency code not recognized in supported ISO codes.
* `INVALID_TIMESTAMP`: Malformed date string failing ISO-8601 parsing.
* `BUSINESS_RULE_VIOLATION`: Status or payment method outside allowable enumerated values.
* `FOREIGN_KEY_VIOLATION`: `merchant_id` does not exist in `merchants.json` or merchant is suspended.
* `DUPLICATE_KEY`: Duplicate `transaction_id` detected within the incoming batch or already published in the Silver Lake.

---

### 2.4 Pipeline Audit Entity: `pipeline_audit_history`
* **Grain**: One row per pipeline execution run.
* **Storage Location**: `data/lake/audit_logs/pipeline_audit_history.jsonl` (and `audit_{run_id}.json`)

| Field | Data Type | Description |
| :--- | :--- | :--- |
| `run_id` | STRING (UUID) | Unique execution identifier. |
| `batch_id` | STRING | Logical identifier of the processed batch. |
| `start_time` | STRING (ISO-8601) | Run invocation timestamp. |
| `end_time` | STRING (ISO-8601) | Run termination timestamp. |
| `duration_ms` | FLOAT | Total execution latency in milliseconds. |
| `input_records_count` | INT | Count of raw records read from input source. |
| `valid_records_count` | INT | Count of records passing validation and published. |
| `rejected_records_count`| INT | Count of records routed to quarantine. |
| `published_records_count`| INT | Count of records written to Silver Parquet partitions. |
| `deduped_records_count` | INT | Count of files/records consolidated during compaction. |
| `partitions_affected` | LIST[STRING] | Array of `event_date` partitions touched during the run. |
| `status` | STRING | Execution outcome: `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`. |
| `error_summary` | STRING / NULL | Root cause description if failure occurred. |
| `quarantine_breakdown` | DICT | Key-value mapping of rejection counts by reason. |
