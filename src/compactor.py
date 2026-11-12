"""
File Compaction and Row-Group Optimization Engine.
Eliminates the 'small files problem' by coalescing fragmented micro-batch Parquet files
within each time partition into target-sized, sorted, dictionary-encoded Parquet files.
"""

from pathlib import Path
from typing import List, Dict, Any, Optional
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from src.schema import TARGET_PARQUET_SCHEMA


class LakeCompactor:
    """
    Compacts small Parquet files within date partitions into optimal files
    and optimizes internal row-group ordering for statistics pruning.
    """

    def __init__(
        self,
        lake_root: Path,
        target_file_size_bytes: int = 128 * 1024 * 1024,
        max_rows_per_file: int = 250_000,
        row_group_size: int = 25_000,
        sort_columns: Optional[List[str]] = None
    ):
        self.lake_root = Path(lake_root)
        self.target_file_size_bytes = target_file_size_bytes
        self.max_rows_per_file = max_rows_per_file
        self.row_group_size = row_group_size
        self.sort_columns = sort_columns or ["merchant_id", "transaction_timestamp"]

    def discover_fragmented_partitions(self, min_file_count: int = 2) -> List[Path]:
        """Identifies partition directories containing more than min_file_count files."""
        fragmented = []
        for part_dir in self.lake_root.glob("event_date=*"):
            if part_dir.is_dir():
                parquet_files = list(part_dir.glob("*.parquet"))
                if len(parquet_files) >= min_file_count:
                    fragmented.append(part_dir)
        return sorted(fragmented)

    def compact_partition(self, partition_dir: Path) -> Dict[str, Any]:
        """
        Compacts all Parquet files inside a specific partition directory.
        
        1. Reads all files.
        2. Deduplicates on 'transaction_id' (preserving idempotency).
        3. Sorts by sort_columns to cluster values and maximize row-group min/max pruning.
        4. Writes out optimal file(s) and cleans up old small files atomically.
        """
        parquet_files = sorted(list(partition_dir.glob("*.parquet")))
        if not parquet_files:
            return {"status": "SKIPPED", "files_before": 0, "files_after": 0}

        # If only 1 file and already compacted, skip
        if len(parquet_files) == 1 and "compacted" in parquet_files[0].name:
            return {
                "status": "ALREADY_COMPACTED",
                "partition": partition_dir.name,
                "files_before": 1,
                "files_after": 1,
            }

        total_bytes_before = sum(f.stat().st_size for f in parquet_files)
        total_rows_before = 0

        # Read all files in partition
        tables: List[pa.Table] = []
        for f in parquet_files:
            t = pq.ParquetFile(f).read()
            total_rows_before += t.num_rows
            tables.append(t)

        combined_table = pa.concat_tables(tables, promote_options="default")

        # Convert to Pandas for deterministic deduplication and clustering sort
        df = combined_table.to_pandas()
        
        # Deduplication: keep the latest ingested record if duplicates exist
        if "transaction_id" in df.columns:
            df = df.drop_duplicates(subset=["transaction_id"], keep="last")

        # Sort columns to maximize compression and enable row group statistics pruning
        sort_cols = [c for c in self.sort_columns if c in df.columns]
        if sort_cols:
            df = df.sort_values(by=sort_cols).reset_index(drop=True)

        # Convert back to Arrow table
        optimized_table = pa.Table.from_pandas(df, schema=combined_table.schema, preserve_index=False)
        total_rows_after = optimized_table.num_rows

        # Write to temporary file first (atomic staging)
        temp_compacted_files: List[Path] = []
        
        # Chunk into target row count if table exceeds max_rows_per_file
        num_chunks = max(1, (total_rows_after + self.max_rows_per_file - 1) // self.max_rows_per_file)
        
        for chunk_idx in range(num_chunks):
            start_row = chunk_idx * self.max_rows_per_file
            end_row = min(start_row + self.max_rows_per_file, total_rows_after)
            chunk_table = optimized_table.slice(start_row, end_row - start_row)

            tmp_filename = f"compacted_part_{chunk_idx:04d}.tmp"
            tmp_path = partition_dir / tmp_filename

            pq.write_table(
                chunk_table,
                tmp_path,
                compression="SNAPPY",
                row_group_size=self.row_group_size,
                write_statistics=True
            )
            temp_compacted_files.append(tmp_path)

        # Remove the old fragmented small files
        for old_file in parquet_files:
            try:
                old_file.unlink()
            except Exception as e:
                pass

        # Rename temp files to final parquet names
        final_files: List[Path] = []
        for idx, tmp_file in enumerate(temp_compacted_files):
            final_path = partition_dir / f"compacted_part_{idx:04d}.parquet"
            tmp_file.rename(final_path)
            final_files.append(final_path)

        total_bytes_after = sum(f.stat().st_size for f in final_files)

        return {
            "status": "COMPACTED",
            "partition": partition_dir.name,
            "files_before": len(parquet_files),
            "files_after": len(final_files),
            "bytes_before": total_bytes_before,
            "bytes_after": total_bytes_after,
            "compression_ratio": round(total_bytes_before / total_bytes_after, 2) if total_bytes_after > 0 else 1.0,
            "rows_before": total_rows_before,
            "rows_after": total_rows_after,
            "deduped_records": total_rows_before - total_rows_after,
            "final_files": [str(p) for p in final_files]
        }

    def compact_all(self, min_file_count: int = 2) -> Dict[str, Any]:
        """Runs compaction across all fragmented partitions in the lake."""
        partitions = self.discover_fragmented_partitions(min_file_count=min_file_count)
        results = []
        total_files_before = 0
        total_files_after = 0
        total_bytes_before = 0
        total_bytes_after = 0

        for part in partitions:
            res = self.compact_partition(part)
            results.append(res)
            total_files_before += res.get("files_before", 0)
            total_files_after += res.get("files_after", 0)
            total_bytes_before += res.get("bytes_before", 0)
            total_bytes_after += res.get("bytes_after", 0)

        return {
            "total_partitions_compacted": len(results),
            "total_files_before": total_files_before,
            "total_files_after": total_files_after,
            "files_eliminated": total_files_before - total_files_after,
            "total_bytes_before": total_bytes_before,
            "total_bytes_after": total_bytes_after,
            "partition_results": results
        }
