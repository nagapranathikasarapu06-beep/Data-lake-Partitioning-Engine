"""
Pipeline Audit, Logging, and Operational Observability Engine.
Tracks run metadata, start/end timestamps, input/output row counts, rejection counts,
execution status, and failure causes.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict, field


@dataclass
class RunMetrics:
    run_id: str
    batch_id: str
    start_time: str
    end_time: Optional[str] = None
    duration_ms: float = 0.0
    input_records_count: int = 0
    valid_records_count: int = 0
    rejected_records_count: int = 0
    published_records_count: int = 0
    deduped_records_count: int = 0
    partitions_affected: List[str] = field(default_factory=list)
    status: str = "PENDING"  # SUCCESS, PARTIAL_SUCCESS, FAILED
    error_summary: Optional[str] = None
    quarantine_breakdown: Dict[str, int] = field(default_factory=dict)


class PipelineAuditLogger:
    """Manages audit trails and operational logging."""

    def __init__(self, audit_dir: Path):
        self.audit_dir = Path(audit_dir)
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.master_log_path = self.audit_dir / "pipeline_audit_history.jsonl"
        self._setup_logger()

    def _setup_logger(self):
        self.logger = logging.getLogger("DataLakeEngine")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] [run_id=%(run_id)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%SZ"
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def log(self, level: int, msg: str, run_id: str = "N/A"):
        extra = {"run_id": run_id}
        self.logger.log(level, msg, extra=extra)

    def save_run_metrics(self, metrics: RunMetrics) -> Path:
        """Persists run metrics to a dedicated run file and appends to the master log."""
        data = asdict(metrics)
        
        # Individual run file
        run_file = self.audit_dir / f"audit_{metrics.run_id}.json"
        with open(run_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        # Master append-only log
        with open(self.master_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(data) + "\n")

        return run_file

    def get_run_history(self) -> List[Dict[str, Any]]:
        """Retrieves history of all recorded pipeline runs."""
        if not self.master_log_path.exists():
            return []
        runs = []
        with open(self.master_log_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    runs.append(json.loads(line.strip()))
        return runs
