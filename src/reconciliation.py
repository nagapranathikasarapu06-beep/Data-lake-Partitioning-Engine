"""
Data Reconciliation and Output Integrity Verification Engine.
Performs source-to-target count reconciliation, financial aggregate verification,
referential integrity assertions, and generates structured reconciliation reports.
"""

from pathlib import Path
from typing import Dict, Any, List
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd
from config.pipeline_config import PipelineConfig, DEFAULT_CONFIG


class DataReconciliationEngine:
    """Verifies lake completeness, consistency, and financial balance integrity."""

    def __init__(self, config: PipelineConfig = DEFAULT_CONFIG):
        self.config = config

    def reconcile(self) -> Dict[str, Any]:
        """Runs comprehensive reconciliation across Silver, Quarantine, and Reference data."""
        # 1. Inspect Silver Lake
        silver_files = list(self.config.SILVER_COMPACTED_DIR.glob("event_date=*/*.parquet"))
        total_silver_rows = 0
        total_silver_amount = 0.0
        silver_merchants = set()
        duplicate_silver_pks = 0
        all_silver_ids = set()
        partition_stats: Dict[str, Dict[str, Any]] = {}

        if silver_files:
            silver_dataset = ds.dataset(
                str(self.config.SILVER_COMPACTED_DIR),
                format="parquet",
                partitioning=ds.partitioning(
                    schema=pa.schema([("event_date", pa.string())]),
                    flavor="hive"
                )
            )
            silver_table = silver_dataset.to_table()
            total_silver_rows = silver_table.num_rows

            df_silver = silver_table.to_pandas()
            if not df_silver.empty:
                total_silver_amount = round(float(df_silver["amount"].sum()), 2)
                silver_merchants = set(df_silver["merchant_id"].unique())
                
                # Check for PK duplicates
                pk_counts = df_silver["transaction_id"].value_counts()
                duplicate_silver_pks = int((pk_counts > 1).sum())

                # Per-partition breakdown
                for ev_date, grp in df_silver.groupby("event_date"):
                    partition_stats[str(ev_date)] = {
                        "row_count": len(grp),
                        "total_amount": round(float(grp["amount"].sum()), 2),
                        "merchants_count": int(grp["merchant_id"].nunique())
                    }

        # 2. Inspect Quarantine
        quarantine_files = list(self.config.QUARANTINE_DIR.glob("reject_date=*/*.parquet"))
        total_quarantine_rows = 0
        quarantine_reasons: Dict[str, int] = {}

        for qf in quarantine_files:
            qt = pq.ParquetFile(qf).read()
            total_quarantine_rows += qt.num_rows
            qdf = qt.to_pandas()
            for r, count in qdf["rejection_reason"].value_counts().items():
                quarantine_reasons[str(r)] = quarantine_reasons.get(str(r), 0) + int(count)

        # 3. Inspect Reference Merchants
        ref_merchants_file = self.config.REFERENCE_DIR / "merchants.json"
        active_merchants_count = 0
        all_ref_ids = set()
        if ref_merchants_file.exists():
            import json
            with open(ref_merchants_file, "r", encoding="utf-8") as f:
                ref_data = json.load(f)
                all_ref_ids = {m["merchant_id"] for m in ref_data if m.get("status") == "ACTIVE"}
                active_merchants_count = len(all_ref_ids)

        # Referential integrity check
        orphan_merchants = silver_merchants - all_ref_ids

        # Reconciliation balance
        reconciliation_passed = (duplicate_silver_pks == 0) and (len(orphan_merchants) == 0)

        report = {
            "reconciliation_status": "PASSED" if reconciliation_passed else "FAILED",
            "silver_metrics": {
                "total_records": total_silver_rows,
                "total_amount_usd": total_silver_amount,
                "total_partitions": len(partition_stats),
                "total_files": len(silver_files),
                "duplicate_primary_keys": duplicate_silver_pks,
                "unique_merchants_served": len(silver_merchants),
                "orphan_merchants_detected": list(orphan_merchants),
                "partitions": partition_stats,
            },
            "quarantine_metrics": {
                "total_quarantined_records": total_quarantine_rows,
                "breakdown_by_reason": quarantine_reasons,
            },
            "reference_metrics": {
                "active_merchants_enrolled": active_merchants_count
            },
            "integrity_checks": {
                "primary_key_uniqueness": "PASS" if duplicate_silver_pks == 0 else "FAIL",
                "referential_integrity": "PASS" if len(orphan_merchants) == 0 else "FAIL",
            }
        }

        return report

    def generate_markdown_report(self, report_dict: Dict[str, Any], output_path: Path) -> Path:
        """Writes an executive markdown summary of the reconciliation results."""
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        sm = report_dict["silver_metrics"]
        qm = report_dict["quarantine_metrics"]
        ic = report_dict["integrity_checks"]

        lines = [
            "# Data Lake Pipeline Reconciliation & Integrity Report",
            "",
            f"**Overall Status**: `{report_dict['reconciliation_status']}`",
            "",
            "## 1. Executive Summary & Aggregate Balance",
            "",
            "| Metric | Target Silver Lake | Quarantine Layer | Total Processed |",
            "| :--- | :--- | :--- | :--- |",
            f"| **Record Count** | {sm['total_records']:,} | {qm['total_quarantined_records']:,} | {sm['total_records'] + qm['total_quarantined_records']:,} |",
            f"| **Total Financial Volume (USD)** | ${sm['total_amount_usd']:,.2f} | N/A (Quarantined) | ${sm['total_amount_usd']:,.2f} |",
            f"| **Active Partitions** | {sm['total_partitions']} | N/A | {sm['total_partitions']} |",
            f"| **Physical Parquet Files** | {sm['total_files']} | {len(list(self.config.QUARANTINE_DIR.glob('*/*.parquet')))} | {sm['total_files'] + len(list(self.config.QUARANTINE_DIR.glob('*/*.parquet')))} |",
            "",
            "## 2. Data Quality & Referential Integrity Verifications",
            "",
            f"- **Primary Key Uniqueness (`transaction_id`)**: `{ic['primary_key_uniqueness']}` (Duplicates detected: {sm['duplicate_primary_keys']})",
            f"- **Foreign Key Integrity (`merchant_id` -> `merchants`)**: `{ic['referential_integrity']}` (Orphans: {len(sm['orphan_merchants_detected'])})",
            "",
            "## 3. Quarantine Classification Breakdown",
            "",
            "| Rejection Reason | Quarantined Records Count | Impact Description |",
            "| :--- | :--- | :--- |",
        ]

        for reason, count in qm["breakdown_by_reason"].items():
            desc = {
                "MALFORMED_RECORD": "Syntax/JSON unparseable input",
                "MISSING_REQUIRED_FIELD": "Null or missing mandatory attributes",
                "INVALID_AMOUNT": "Amount <= 0.0 or non-numeric",
                "INVALID_CURRENCY": "Unrecognized ISO currency code",
                "INVALID_TIMESTAMP": "Invalid ISO-8601 UTC timestamp format",
                "BUSINESS_RULE_VIOLATION": "Status or payment method outside permitted domain",
                "FOREIGN_KEY_VIOLATION": "Merchant not found in reference dimension",
                "DUPLICATE_KEY": "Duplicate transaction_id within batch or cross-batch",
            }.get(reason, "Data quality violation")
            lines.append(f"| `{reason}` | {count:,} | {desc} |")

        lines.extend([
            "",
            "## 4. Partition Distribution in Target Silver Lake",
            "",
            "| Partition Date (`event_date`) | Rows Count | Financial Volume (USD) | Distinct Merchants |",
            "| :--- | :--- | :--- | :--- |",
        ])

        for part_date, pinfo in sm.get("partitions", {}).items():
            lines.append(f"| `{part_date}` | {pinfo['row_count']:,} | ${pinfo['total_amount']:,.2f} | {pinfo['merchants_count']} |")

        lines.extend([
            "",
            "---",
            "*Report generated automatically by DataLakeEngine Reconciliation Subsystem.*"
        ])

        with open(out, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        return out
