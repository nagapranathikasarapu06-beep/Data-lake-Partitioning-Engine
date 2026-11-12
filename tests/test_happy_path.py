"""
Automated Tests: Happy Path Data Ingestion and Partitioning.
"""

import unittest
import tempfile
import shutil
import json
from pathlib import Path
from datetime import datetime, timezone

from config.pipeline_config import PipelineConfig
from src.ingestion import PipelineEngine
from src.partitioner import LakePartitioner
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa


class TestHappyPath(unittest.TestCase):
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

    def test_successful_ingestion_and_partitioning(self):
        valid_records = [
            {
                "transaction_id": "TX_001",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_101",
                "amount": 150.75,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T14:30:00Z"
            },
            {
                "transaction_id": "TX_002",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_102",
                "amount": 49.99,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "DEBIT_CARD",
                "transaction_timestamp": "2026-11-10T15:00:00Z"
            },
            {
                "transaction_id": "TX_003",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_103",
                "amount": 200.00,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "WIRE_TRANSFER",
                "transaction_timestamp": "2026-11-11T09:00:00Z"
            }
        ]

        batch_file = self.config.SAMPLE_INPUTS_DIR / "happy_batch.json"
        with open(batch_file, "w", encoding="utf-8") as f:
            json.dump(valid_records, f)

        metrics = self.engine.run_batch(batch_file)

        # Assertions on metrics
        self.assertEqual(metrics.status, "SUCCESS")
        self.assertEqual(metrics.input_records_count, 3)
        self.assertEqual(metrics.valid_records_count, 3)
        self.assertEqual(metrics.rejected_records_count, 0)
        self.assertEqual(metrics.published_records_count, 3)
        self.assertEqual(metrics.partitions_affected, ["2026-11-10", "2026-11-11"])

        # Check physical partition layout
        part_10 = self.config.SILVER_COMPACTED_DIR / "event_date=2026-11-10"
        part_11 = self.config.SILVER_COMPACTED_DIR / "event_date=2026-11-11"
        self.assertTrue(part_10.exists())
        self.assertTrue(part_11.exists())

        # Inspect parquet data
        f_10 = list(part_10.glob("*.parquet"))[0]
        t_10 = pq.ParquetFile(f_10).read()
        self.assertEqual(t_10.num_rows, 2)

        f_11 = list(part_11.glob("*.parquet"))[0]
        t_11 = pq.ParquetFile(f_11).read()
        self.assertEqual(t_11.num_rows, 1)


if __name__ == "__main__":
    unittest.main()
