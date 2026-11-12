"""
Core Ingestion and Pipeline Execution Engine.
Orchestrates file reading, schema validation, transformation, partitioning,
quarantine routing, operational auditing, and automated compaction.
"""

import json
import uuid
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set
import pyarrow as pa
import pyarrow.parquet as pq

from config.pipeline_config import PipelineConfig, DEFAULT_CONFIG
from src.schema import MerchantRecord, QUARANTINE_PARQUET_SCHEMA
from src.validator import DataValidator
from src.transformer import DataTransformer
from src.partitioner import LakePartitioner
from src.compactor import LakeCompactor
from src.audit import PipelineAuditLogger, RunMetrics


class PipelineEngine:
    """End-to-end ingestion and lake management engine."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or DEFAULT_CONFIG
        self.config.ensure_directories()
        
        self.audit_logger = PipelineAuditLogger(self.config.AUDIT_DIR)
        self.partitioner = LakePartitioner(self.config.SILVER_COMPACTED_DIR)
        self.compactor = LakeCompactor(
            lake_root=self.config.SILVER_COMPACTED_DIR,
            target_file_size_bytes=self.config.TARGET_FILE_SIZE_BYTES,
            max_rows_per_file=self.config.MAX_ROWS_PER_COMPACTED_FILE,
            row_group_size=self.config.TARGET_ROW_GROUP_SIZE,
            sort_columns=self.config.SORT_COLUMNS
        )
        
        # Load active merchants reference set for referential integrity
        self.valid_merchants = self._load_reference_merchants()
        self.validator = DataValidator(valid_merchant_ids=set(self.valid_merchants.keys()))

    def _load_reference_merchants(self) -> Dict[str, MerchantRecord]:
        """Loads reference merchants from reference store."""
        ref_file = self.config.REFERENCE_DIR / "merchants.json"
        if not ref_file.exists():
            return {}
        with open(ref_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        merchants = {}
        for m in data:
            rec = MerchantRecord(**m)
            if rec.status == "ACTIVE":
                merchants[rec.merchant_id] = rec
        return merchants

    def run_batch(
        self,
        input_file: Path,
        batch_id: Optional[str] = None,
        auto_compact: bool = True
    ) -> RunMetrics:
        """
        Executes ingestion for a single input batch file.
        
        Args:
            input_file: Path to input JSON or JSONL file.
            batch_id: Optional batch identifier; if not provided, generated deterministically.
            auto_compact: Whether to run partition compaction immediately after batch load.
        """
        start_time_dt = datetime.now(timezone.utc)
        start_time_iso = start_time_dt.isoformat()
        t0 = time.perf_counter()

        input_path = Path(input_file)
        batch_id = batch_id or f"batch_{input_path.stem}"
        run_id = str(uuid.uuid4())

        self.audit_logger.log(20, f"Starting ingestion for file: {input_path.name}", run_id=run_id)

        metrics = RunMetrics(
            run_id=run_id,
            batch_id=batch_id,
            start_time=start_time_iso,
        )

        try:
            # 1. Read input records
            records = self._read_input_file(input_path)
            metrics.input_records_count = len(records)

            if len(records) == 0:
                self.audit_logger.log(20, "Batch is empty. Completing gracefully.", run_id=run_id)
                metrics.status = "SUCCESS"
                metrics.end_time = datetime.now(timezone.utc).isoformat()
                metrics.duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
                self.audit_logger.save_run_metrics(metrics)
                return metrics

            # 2. Collect existing transaction IDs across the silver lake to enforce idempotency
            existing_tx_ids = self._get_existing_transaction_ids()

            # 3. Validation & Referential Integrity
            valid_raw, quarantined = self.validator.validate_batch(
                records=records,
                run_id=run_id,
                source_file=input_path.name,
                existing_seen_ids=existing_tx_ids
            )
            metrics.valid_records_count = len(valid_raw)
            metrics.rejected_records_count = len(quarantined)

            # Record breakdown of rejection causes
            for q in quarantined:
                metrics.quarantine_breakdown[q.rejection_reason] = (
                    metrics.quarantine_breakdown.get(q.rejection_reason, 0) + 1
                )

            # 4. Handle Quarantine Output
            if quarantined:
                self._write_quarantine(quarantined, run_id=run_id, batch_id=batch_id)

            # 5. Transform Valid Records
            if valid_raw:
                transformed = DataTransformer.transform_batch(
                    valid_raw,
                    run_id=run_id,
                    source_file=input_path.name
                )
                arrow_table = DataTransformer.to_arrow_table(transformed)
                
                # Derive affected partitions
                partitions = set(arrow_table.column("event_date").unique().to_pylist())
                metrics.partitions_affected = sorted(list(partitions))

                # 6. Publish to Silver Lake (partitioned)
                written_files = self.partitioner.write_partitioned_batch(
                    table=arrow_table,
                    batch_id=batch_id
                )
                metrics.published_records_count = arrow_table.num_rows

                # 7. Optional Compaction
                if auto_compact:
                    compact_summary = self.compactor.compact_all()
                    metrics.deduped_records_count = compact_summary.get("total_files_before", 0) - compact_summary.get("total_files_after", 0)

            # Finalize status
            if metrics.rejected_records_count > 0 and metrics.valid_records_count > 0:
                metrics.status = "PARTIAL_SUCCESS"
            elif metrics.rejected_records_count > 0 and metrics.valid_records_count == 0:
                metrics.status = "FAILED"
                metrics.error_summary = "All records in batch were rejected by validation."
            else:
                metrics.status = "SUCCESS"

        except Exception as e:
            metrics.status = "FAILED"
            metrics.error_summary = str(e)
            self.audit_logger.log(40, f"Pipeline run failed with error: {e}", run_id=run_id)

        finally:
            t1 = time.perf_counter()
            metrics.end_time = datetime.now(timezone.utc).isoformat()
            metrics.duration_ms = round((t1 - t0) * 1000.0, 2)
            self.audit_logger.save_run_metrics(metrics)
            self.audit_logger.log(
                20,
                f"Completed run {run_id} with status={metrics.status}, "
                f"input={metrics.input_records_count}, valid={metrics.valid_records_count}, "
                f"rejected={metrics.rejected_records_count}, duration={metrics.duration_ms}ms",
                run_id=run_id
            )

        return metrics

    def _read_input_file(self, path: Path) -> List[Any]:
        """Reads JSON array or JSON-lines file."""
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")

        records = []
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return []
            if content.startswith("["):
                try:
                    records = json.loads(content)
                except Exception as e:
                    # If JSON parsing as whole fails, yield as malformed raw string
                    records = [{"__raw_malformed__": content, "error": str(e)}]
            else:
                for line in content.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        records.append(line)
        return records

    def _write_quarantine(self, quarantined_records: List[Any], run_id: str, batch_id: str):
        """Persists quarantined records as traceable Parquet and JSON files."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        quarantine_dir = self.config.QUARANTINE_DIR / f"reject_date={today}"
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        # JSON dump for quick human inspection
        json_path = quarantine_dir / f"quarantine_{batch_id}_{run_id[:8]}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump([q.model_dump() for q in quarantined_records], f, indent=2)

        # Parquet dump for scalable analytical querying
        data = {
            "raw_record": [q.raw_record for q in quarantined_records],
            "rejection_reason": [q.rejection_reason for q in quarantined_records],
            "error_details": [q.error_details for q in quarantined_records],
            "run_id": [q.run_id for q in quarantined_records],
            "source_file": [q.source_file for q in quarantined_records],
            "rejected_at": [q.rejected_at for q in quarantined_records],
        }
        table = pa.Table.from_pydict(data, schema=QUARANTINE_PARQUET_SCHEMA)
        parquet_path = quarantine_dir / f"quarantine_{batch_id}_{run_id[:8]}.parquet"
        pq.write_table(table, parquet_path, compression="SNAPPY")

    def _get_existing_transaction_ids(self) -> Set[str]:
        """Gathers already published transaction IDs to prevent cross-batch duplicate keys."""
        existing_ids = set()
        for pfile in self.config.SILVER_COMPACTED_DIR.glob("event_date=*/*.parquet"):
            try:
                # Read only transaction_id column for high efficiency using ParquetFile
                t = pq.ParquetFile(pfile).read(columns=["transaction_id"])
                existing_ids.update(t.column("transaction_id").to_pylist())
            except Exception:
                pass
        return existing_ids
