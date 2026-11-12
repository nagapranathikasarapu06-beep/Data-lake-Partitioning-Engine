"""
Automated Tests: Partition Pruning and Compaction Behavior.
"""

import unittest
import tempfile
import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from config.pipeline_config import PipelineConfig
from src.partitioner import LakePartitioner
from src.compactor import LakeCompactor
from src.schema import TARGET_PARQUET_SCHEMA


class TestPartitionPruningAndCompaction(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.lake_dir = self.test_dir / "lake"
        self.partitioner = LakePartitioner(self.lake_dir)
        self.compactor = LakeCompactor(self.lake_dir, max_rows_per_file=50_000, row_group_size=1_000)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _create_mock_batch(self, event_date: str, count: int, batch_id: str) -> pa.Table:
        data = {
            "transaction_id": [f"TX_{batch_id}_{i}" for i in range(count)],
            "merchant_id": ["MERC_0001"] * count,
            "customer_id": [f"CUST_{i}" for i in range(count)],
            "amount": [10.0 + i for i in range(count)],
            "currency": ["USD"] * count,
            "status": ["COMPLETED"] * count,
            "payment_method": ["CREDIT_CARD"] * count,
            "transaction_timestamp": [f"{event_date}T10:00:00Z"] * count,
            "event_date": [event_date] * count,
            "event_year": [int(event_date.split("-")[0])] * count,
            "event_month": [int(event_date.split("-")[1])] * count,
            "run_id": [f"run_{batch_id}"] * count,
            "source_file": [f"file_{batch_id}.json"] * count,
            "ingested_at": ["2026-11-12T00:00:00Z"] * count,
        }
        return pa.Table.from_pydict(data, schema=TARGET_PARQUET_SCHEMA)

    def test_partition_pruning_scan(self):
        # Write data across 3 separate dates
        dates = ["2026-11-01", "2026-11-02", "2026-11-03"]
        for d in dates:
            tbl = self._create_mock_batch(d, 50, batch_id=d)
            self.partitioner.write_partitioned_batch(tbl, batch_id=d)

        # Scan with filter on event_date == '2026-11-02'
        scan_info = self.partitioner.inspect_query_scan(
            filter_expr=ds.field("event_date") == "2026-11-02"
        )

        self.assertEqual(scan_info["total_files"], 3)
        self.assertEqual(scan_info["scanned_files_count"], 1)
        self.assertEqual(scan_info["pruned_files_count"], 2)
        self.assertAlmostEqual(scan_info["pruning_ratio_pct"], 66.67, delta=0.1)
        self.assertEqual(scan_info["rows_returned"], 50)

    def test_compaction_coalesces_small_files(self):
        target_date = "2026-11-05"
        # Simulate 5 micro-batches arriving for the same date
        for b_idx in range(5):
            tbl = self._create_mock_batch(target_date, 20, batch_id=f"micro_{b_idx}")
            self.partitioner.write_partitioned_batch(tbl, batch_id=f"micro_{b_idx}")

        part_dir = self.lake_dir / f"event_date={target_date}"
        initial_files = list(part_dir.glob("*.parquet"))
        self.assertEqual(len(initial_files), 5)

        # Run compaction on partition
        res = self.compactor.compact_partition(part_dir)
        self.assertEqual(res["status"], "COMPACTED")
        self.assertEqual(res["files_before"], 5)
        self.assertEqual(res["files_after"], 1)
        self.assertEqual(res["rows_after"], 100)

        compacted_files = list(part_dir.glob("*.parquet"))
        self.assertEqual(len(compacted_files), 1)
        self.assertTrue(compacted_files[0].name.startswith("compacted_part_"))


if __name__ == "__main__":
    unittest.main()
