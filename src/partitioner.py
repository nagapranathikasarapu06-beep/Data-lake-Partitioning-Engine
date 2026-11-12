"""
Data Lake Partitioner Module.
Manages Hive-style directory partitioning (event_date=YYYY-MM-DD),
file writing, partition discovery, and inspection of scanned files for pruning verification.
"""

from pathlib import Path
from typing import List, Dict, Any, Optional
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from src.schema import TARGET_PARQUET_SCHEMA


class LakePartitioner:
    """Manages partition-level writes, layout hierarchy, and partition pruning analysis."""

    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def write_partitioned_batch(
        self,
        table: pa.Table,
        batch_id: str,
        preserve_small_files: bool = False
    ) -> List[Path]:
        """
        Writes an Arrow table partitioned by 'event_date'.
        
        Args:
            table: PyArrow table containing records to write.
            batch_id: Deterministic or run identifier used in file naming.
            preserve_small_files: If True, writes as individual small batch files
                                  (simulates streaming/micro-batch ingestion).
        
        Returns:
            List of written Parquet file paths.
        """
        if table.num_rows == 0:
            return []

        written_files: List[Path] = []
        # Group table slices by unique event_date
        event_dates = table.column("event_date").unique().to_pylist()

        for ev_date in event_dates:
            partition_dir = self.root_dir / f"event_date={ev_date}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Filter table for this partition
            mask = pa.compute.equal(table.column("event_date"), ev_date)
            partition_table = table.filter(mask)

            # File naming
            filename = f"batch_{batch_id}_part_000.parquet"
            dest_path = partition_dir / filename

            # Write Parquet with Snappy compression and column statistics enabled
            pq.write_table(
                partition_table,
                dest_path,
                compression="SNAPPY",
                row_group_size=25_000,
                write_statistics=True
            )
            written_files.append(dest_path)

        return written_files

    def write_unpartitioned_batch(
        self,
        table: pa.Table,
        batch_id: str,
        dest_dir: Path
    ) -> Path:
        """
        Writes all records into a single unpartitioned flat directory for baseline comparison.
        """
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / f"unpartitioned_batch_{batch_id}.parquet"
        pq.write_table(
            table,
            dest_path,
            compression="SNAPPY",
            row_group_size=25_000,
            write_statistics=True
        )
        return dest_path

    def get_dataset(self, partitioning: str = "hive") -> ds.Dataset:
        """Loads the partition tree as a PyArrow Dataset."""
        if partitioning == "hive":
            return ds.dataset(
                str(self.root_dir),
                format="parquet",
                partitioning=ds.partitioning(
                    schema=pa.schema([("event_date", pa.string())]),
                    flavor="hive"
                )
            )
        else:
            return ds.dataset(str(self.root_dir), format="parquet")

    def inspect_query_scan(
        self,
        filter_expr: Optional[ds.Expression] = None,
        columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Executes a scan over the dataset and reports detailed pruning metrics:
        total files in lake, scanned files, pruned files, scanned bytes, and row count.
        """
        dataset = self.get_dataset()
        all_fragments = list(dataset.get_fragments())
        total_files = len(all_fragments)

        # Build filtered scanner
        scanner = dataset.scanner(
            columns=columns,
            filter=filter_expr
        )

        scanned_fragments = list(scanner.scan_batches())
        
        # Determine which file fragments were matched
        scanned_file_paths = set()
        for batch in scanned_fragments:
            # batch has fragment reference in PyArrow dataset
            pass

        # Use dataset scanner to extract scanned file list directly
        matched_files = []
        total_bytes_scanned = 0
        total_rows_scanned = 0

        for frag in dataset.get_fragments(filter=filter_expr):
            matched_files.append(frag.path)
            try:
                p = Path(frag.path)
                if p.exists():
                    total_bytes_scanned += p.stat().st_size
            except Exception:
                pass

        # Materialize table to verify rows returned
        result_table = scanner.to_table()
        total_rows_scanned = result_table.num_rows

        pruned_files = total_files - len(matched_files)
        pruning_ratio = (pruned_files / total_files * 100.0) if total_files > 0 else 0.0

        return {
            "total_files": total_files,
            "scanned_files_count": len(matched_files),
            "pruned_files_count": pruned_files,
            "pruning_ratio_pct": round(pruning_ratio, 2),
            "scanned_files": matched_files,
            "total_bytes_scanned": total_bytes_scanned,
            "rows_returned": total_rows_scanned,
        }
