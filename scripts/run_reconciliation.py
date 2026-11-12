"""
Command-Line Interface to execute Data Lake Reconciliation and generate reports.
Usage:
    python scripts/run_reconciliation.py
"""

import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.pipeline_config import DEFAULT_CONFIG
from src.reconciliation import DataReconciliationEngine


def main():
    print("[RECONCILIATION] Running Data Lake Reconciliation Engine...")
    engine = DataReconciliationEngine(DEFAULT_CONFIG)
    report_dict = engine.reconcile()

    print(f"\nReconciliation Status: {report_dict['reconciliation_status']}")
    print(f"Target Silver Records: {report_dict['silver_metrics']['total_records']:,}")
    print(f"Target Silver Volume (USD): ${report_dict['silver_metrics']['total_amount_usd']:,.2f}")
    print(f"Total Partitions: {report_dict['silver_metrics']['total_partitions']}")
    print(f"Total Quarantine Records: {report_dict['quarantine_metrics']['total_quarantined_records']:,}")
    print(f"Integrity Checks: {report_dict['integrity_checks']}")

    report_file = DEFAULT_CONFIG.REPORTS_DIR / "reconciliation_report.md"
    engine.generate_markdown_report(report_dict, report_file)
    print(f"\n[RECONCILIATION] Detailed report written to: {report_file}")


if __name__ == "__main__":
    main()
