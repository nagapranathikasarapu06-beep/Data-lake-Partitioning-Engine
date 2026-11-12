"""
Automated Tests: Idempotency, Rerun Safety, and Late-Arriving Data Routing.
"""

import unittest
import tempfile
import shutil
import json
from pathlib import Path
import pyarrow.parquet as pq

from config.pipeline_config import PipelineConfig
from src.ingestion import PipelineEngine


class TestIdempotencyAndRerun(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.config = PipelineConfig(
            DATA_DIR=self.test_dir / "data",
            REFERENCE_DIR=self.test_dir / "data" / "reference",
            SAMPLE_INPUTS_DIR=self.test_dir / "data" / "sample_inputs",
            BRONZE_DIR=self.test_dir / "data" / "bronze",
            SILVER_DIR=self.test_dir / "data" / "silver",
            SILVER_COMPACTED_DIR=self.test_dir / "data" / "silver" / "partitioned_compacted",
            QUARANTINE_DIR=self.test_dir / "data" / "quarantine",
            AUDIT_DIR=self.test_dir / "data" / "audit_logs",
            REPORTS_DIR=self.test_dir / "reports",
        )
        self.config.ensure_directories()

        # Seed valid reference merchant
        merchants = [
            {
                "merchant_id": "MERC_0001",
                "merchant_name": "Test Merchant 1",
                "country_code": "US",
                "status": "ACTIVE",
                "created_at": "2025-01-01T00:00:00Z"
            }
        ]
        with open(self.config.REFERENCE_DIR / "merchants.json", "w", encoding="utf-8") as f:
            json.dump(merchants, f)

        self.engine = PipelineEngine(self.config)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_idempotent_rerun_prevents_duplicate_lake_entries(self):
        records = [
            {
                "transaction_id": "TX_IDEMPOTENT_001",
                "merchant_id": "MERC_0001",
                "customer_id": "C_1",
                "amount": 250.00,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-08T12:00:00Z"
            }
        ]
        batch_file = self.config.SAMPLE_INPUTS_DIR / "idempotent_batch.json"
        with open(batch_file, "w", encoding="utf-8") as f:
            json.dump(records, f)

        # First run: should succeed
        m1 = self.engine.run_batch(batch_file)
        self.assertEqual(m1.status, "SUCCESS")
        self.assertEqual(m1.published_records_count, 1)

        # Check silver lake row count
        f1 = list(self.config.SILVER_COMPACTED_DIR.glob("event_date=2026-11-08/*.parquet"))[0]
        t1 = pq.ParquetFile(f1).read()
        self.assertEqual(t1.num_rows, 1)

        # Second run: rerun exact same batch
        m2 = self.engine.run_batch(batch_file)
        self.assertEqual(m2.status, "FAILED")
        self.assertEqual(m2.valid_records_count, 0)
        self.assertEqual(m2.rejected_records_count, 1)
        self.assertEqual(m2.quarantine_breakdown["DUPLICATE_KEY"], 1)

        # Check silver lake row count remains strictly 1 (no duplicates inserted)
        f2 = list(self.config.SILVER_COMPACTED_DIR.glob("event_date=2026-11-08/*.parquet"))[0]
        t2 = pq.ParquetFile(f2).read()
        self.assertEqual(t2.num_rows, 1)

    def test_late_arriving_events_routed_to_correct_historical_partition(self):
        records = [
            {
                "transaction_id": "TX_LATE_HISTORICAL",
                "merchant_id": "MERC_0001",
                "customer_id": "C_1",
                "amount": 120.00,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-10-12T16:45:00Z"  # 1 month in the past
            }
        ]
        batch_file = self.config.SAMPLE_INPUTS_DIR / "late_batch.json"
        with open(batch_file, "w", encoding="utf-8") as f:
            json.dump(records, f)

        m = self.engine.run_batch(batch_file)
        self.assertEqual(m.status, "SUCCESS")
        self.assertEqual(m.partitions_affected, ["2026-10-12"])

        # Verify historical directory was created
        target_dir = self.config.SILVER_COMPACTED_DIR / "event_date=2026-10-12"
        self.assertTrue(target_dir.exists())
        f = list(target_dir.glob("*.parquet"))[0]
        t = pq.ParquetFile(f).read()
        self.assertEqual(t.column("event_date")[0].as_py(), "2026-10-12")


if __name__ == "__main__":
    unittest.main()
