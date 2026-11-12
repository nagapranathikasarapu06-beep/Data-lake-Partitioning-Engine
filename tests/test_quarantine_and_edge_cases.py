"""
Automated Tests: Quarantine Handling, Malformed Input, Empty Batches, and Edge Cases.
"""

import unittest
import tempfile
import shutil
import json
from pathlib import Path
import pyarrow.parquet as pq

from config.pipeline_config import PipelineConfig
from src.ingestion import PipelineEngine


class TestQuarantineAndEdgeCases(unittest.TestCase):
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

    def test_empty_input_file(self):
        empty_file = self.config.SAMPLE_INPUTS_DIR / "empty.json"
        with open(empty_file, "w", encoding="utf-8") as f:
            f.write("[]")

        metrics = self.engine.run_batch(empty_file)
        self.assertEqual(metrics.status, "SUCCESS")
        self.assertEqual(metrics.input_records_count, 0)
        self.assertEqual(metrics.valid_records_count, 0)
        self.assertEqual(metrics.rejected_records_count, 0)

    def test_malformed_syntax_input(self):
        malformed_file = self.config.SAMPLE_INPUTS_DIR / "corrupt.json"
        with open(malformed_file, "w", encoding="utf-8") as f:
            f.write("{ INVALID JSON SYNTAX NOT TERMINATED ...")

        metrics = self.engine.run_batch(malformed_file)
        self.assertEqual(metrics.status, "FAILED")
        self.assertEqual(metrics.rejected_records_count, 1)
        self.assertIn("MALFORMED_RECORD", metrics.quarantine_breakdown)

        # Verify physical quarantine files exist
        q_files = list(self.config.QUARANTINE_DIR.glob("*/*.parquet"))
        self.assertGreater(len(q_files), 0)
        qt = pq.ParquetFile(q_files[0]).read()
        self.assertEqual(qt.column("rejection_reason")[0].as_py(), "MALFORMED_RECORD")

    def test_domain_edge_cases_and_traceability(self):
        edge_records = [
            # 1. Negative amount
            {
                "transaction_id": "TX_NEG",
                "merchant_id": "MERC_0001",
                "customer_id": "C_1",
                "amount": -100.0,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            },
            # 2. Invalid merchant
            {
                "transaction_id": "TX_ORPHAN",
                "merchant_id": "MERC_UNREGISTERED",
                "customer_id": "C_2",
                "amount": 50.0,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            },
            # 3. Valid boundary transaction (0.01 cents)
            {
                "transaction_id": "TX_VALID_BOUNDARY",
                "merchant_id": "MERC_0001",
                "customer_id": "C_3",
                "amount": 0.01,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            }
        ]
        batch_file = self.config.SAMPLE_INPUTS_DIR / "edge_batch.json"
        with open(batch_file, "w", encoding="utf-8") as f:
            json.dump(edge_records, f)

        metrics = self.engine.run_batch(batch_file)
        self.assertEqual(metrics.status, "PARTIAL_SUCCESS")
        self.assertEqual(metrics.input_records_count, 3)
        self.assertEqual(metrics.valid_records_count, 1)
        self.assertEqual(metrics.rejected_records_count, 2)
        self.assertEqual(metrics.quarantine_breakdown["INVALID_AMOUNT"], 1)
        self.assertEqual(metrics.quarantine_breakdown["FOREIGN_KEY_VIOLATION"], 1)


if __name__ == "__main__":
    unittest.main()
