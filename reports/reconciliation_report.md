# Data Lake Pipeline Reconciliation & Integrity Report

**Overall Status**: `PASSED`

## 1. Executive Summary & Aggregate Balance

| Metric | Target Silver Lake | Quarantine Layer | Total Processed |
| :--- | :--- | :--- | :--- |
| **Record Count** | 638 | 520 | 1,158 |
| **Total Financial Volume (USD)** | $2,475,960.21 | N/A (Quarantined) | $2,475,960.21 |
| **Active Partitions** | 10 | N/A | 10 |
| **Physical Parquet Files** | 10 | 3 | 13 |

## 2. Data Quality & Referential Integrity Verifications

- **Primary Key Uniqueness (`transaction_id`)**: `PASS` (Duplicates detected: 0)
- **Foreign Key Integrity (`merchant_id` -> `merchants`)**: `PASS` (Orphans: 0)

## 3. Quarantine Classification Breakdown

| Rejection Reason | Quarantined Records Count | Impact Description |
| :--- | :--- | :--- |
| `DUPLICATE_KEY` | 497 | Duplicate transaction_id within batch or cross-batch |
| `MISSING_REQUIRED_FIELD` | 5 | Null or missing mandatory attributes |
| `INVALID_AMOUNT` | 3 | Amount <= 0.0 or non-numeric |
| `FOREIGN_KEY_VIOLATION` | 7 | Merchant not found in reference dimension |
| `INVALID_CURRENCY` | 5 | Unrecognized ISO currency code |
| `INVALID_TIMESTAMP` | 3 | Invalid ISO-8601 UTC timestamp format |

## 4. Partition Distribution in Target Silver Lake

| Partition Date (`event_date`) | Rows Count | Financial Volume (USD) | Distinct Merchants |
| :--- | :--- | :--- | :--- |
| `2026-10-15` | 31 | $22,654.27 | 16 |
| `2026-10-16` | 26 | $16,745.15 | 13 |
| `2026-10-17` | 28 | $23,394.25 | 14 |
| `2026-10-18` | 31 | $1,020,936.30 | 13 |
| `2026-10-19` | 34 | $27,286.24 | 17 |
| `2026-11-01` | 85 | $58,632.98 | 18 |
| `2026-11-02` | 90 | $69,433.57 | 17 |
| `2026-11-03` | 127 | $96,804.42 | 18 |
| `2026-11-04` | 91 | $1,067,323.68 | 18 |
| `2026-11-05` | 95 | $72,749.35 | 18 |

---
*Report generated automatically by DataLakeEngine Reconciliation Subsystem.*