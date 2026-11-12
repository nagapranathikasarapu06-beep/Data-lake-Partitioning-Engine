"""
Pipeline Configuration Module.
Defines paths, lake layout directories, target file-sizing thresholds, and operational parameters.
"""

from pathlib import Path
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class PipelineConfig:
    # Base paths
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    DATA_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data")
    
    # Storage layers
    REFERENCE_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "reference")
    SAMPLE_INPUTS_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "sample_inputs")
    BRONZE_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "lake" / "bronze")
    SILVER_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "lake" / "silver")
    QUARANTINE_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "lake" / "quarantine")
    AUDIT_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "lake" / "audit_logs")
    REPORTS_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "reports")
    
    # Lake sub-layouts for comparison benchmarks
    SILVER_COMPACTED_DIR: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "lake" / "silver" / "partitioned_compacted"
    )
    SILVER_SMALL_FILES_DIR: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "lake" / "silver" / "partitioned_small_files"
    )
    SILVER_UNPARTITIONED_DIR: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent / "data" / "lake" / "silver" / "unpartitioned"
    )
    
    # Partitioning & Sizing Strategy Settings
    PARTITION_COLUMNS: List[str] = field(default_factory=lambda: ["event_date"])
    SORT_COLUMNS: List[str] = field(default_factory=lambda: ["merchant_id", "transaction_timestamp"])
    
    # File-sizing thresholds
    # In cloud object storage (S3/GCS/ADLS), optimal Parquet sizes are 64 MB - 256 MB (default target 128 MB).
    # For micro-batch benchmark simulations, we also specify target row limits.
    TARGET_FILE_SIZE_BYTES: int = 128 * 1024 * 1024  # 128 MB
    MAX_ROWS_PER_COMPACTED_FILE: int = 250_000
    TARGET_ROW_GROUP_SIZE: int = 25_000
    PARQUET_COMPRESSION: str = "SNAPPY"
    
    # Reproducibility seed
    DEFAULT_RANDOM_SEED: int = 42

    def ensure_directories(self) -> None:
        """Create all required directory paths if they do not exist."""
        for path in [
            self.DATA_DIR,
            self.REFERENCE_DIR,
            self.SAMPLE_INPUTS_DIR,
            self.BRONZE_DIR,
            self.SILVER_DIR,
            self.SILVER_COMPACTED_DIR,
            self.SILVER_SMALL_FILES_DIR,
            self.SILVER_UNPARTITIONED_DIR,
            self.QUARANTINE_DIR,
            self.AUDIT_DIR,
            self.REPORTS_DIR,
        ]:
            path.mkdir(parents=True, exist_ok=True)


DEFAULT_CONFIG = PipelineConfig()
