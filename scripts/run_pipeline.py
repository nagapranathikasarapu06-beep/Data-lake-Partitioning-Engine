"""
Command-Line Interface to execute the Data Lake Ingestion & Partitioning Pipeline.
Usage:
    python scripts/run_pipeline.py [--input-file PATH] [--auto-compact]
"""

import sys
import argparse
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.pipeline_config import DEFAULT_CONFIG
from src.ingestion import PipelineEngine
from generator.dataset_generator import DatasetGenerator


def main():
    parser = argparse.ArgumentParser(description="Data Lake Ingestion & Partitioning Pipeline")
    parser.add_argument(
        "--input-file",
        type=str,
        default=None,
        help="Path to input JSON/JSONL batch file. If omitted, generates and runs all standard samples."
    )
    parser.add_argument(
        "--no-compact",
        action="store_true",
        help="Disable automatic file compaction post-ingestion."
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean existing silver, quarantine, and audit data before running."
    )
    args = parser.parse_args()

    if args.clean:
        import shutil
        print("[PIPELINE] Cleaning existing lake data for fresh execution...")
        for p in [DEFAULT_CONFIG.SILVER_COMPACTED_DIR, DEFAULT_CONFIG.QUARANTINE_DIR, DEFAULT_CONFIG.AUDIT_DIR]:
            shutil.rmtree(p, ignore_errors=True)
        DEFAULT_CONFIG.ensure_directories()

    engine = PipelineEngine(DEFAULT_CONFIG)

    if args.input_file:
        file_path = Path(args.input_file)
        print(f"[PIPELINE] Running ingestion for: {file_path}")
        metrics = engine.run_batch(file_path, auto_compact=not args.no_compact)
        print(f"[PIPELINE] Result: status={metrics.status}, input={metrics.input_records_count}, "
              f"valid={metrics.valid_records_count}, rejected={metrics.rejected_records_count}")
    else:
        print("[PIPELINE] No input file specified. Generating standard test batches...")
        gen = DatasetGenerator(seed=42, config=DEFAULT_CONFIG)
        batch_paths = gen.generate_all_sample_batches()

        # Run starter batch
        print("\n=== Running Starter Batch ===")
        m1 = engine.run_batch(batch_paths["starter_batch"], auto_compact=not args.no_compact)
        print(f"Starter Batch Result: status={m1.status}, valid={m1.valid_records_count}, rejected={m1.rejected_records_count}")

        # Run negative edge-cases batch
        print("\n=== Running Negative Edge Cases Batch ===")
        m2 = engine.run_batch(batch_paths["edge_cases_batch"], auto_compact=not args.no_compact)
        print(f"Edge Cases Batch Result: status={m2.status}, valid={m2.valid_records_count}, rejected={m2.rejected_records_count}")
        print(f"Rejections breakdown: {m2.quarantine_breakdown}")

        # Run late-arriving batch
        print("\n=== Running Late-Arriving Batch ===")
        m3 = engine.run_batch(batch_paths["late_arriving_batch"], auto_compact=not args.no_compact)
        print(f"Late Arriving Batch Result: status={m3.status}, valid={m3.valid_records_count}, rejected={m3.rejected_records_count}")
        print(f"Partitions affected: {m3.partitions_affected}")

        # Test Idempotent Rerun of Starter Batch
        print("\n=== Testing Idempotent Rerun of Starter Batch ===")
        m4 = engine.run_batch(batch_paths["starter_batch"], auto_compact=not args.no_compact)
        print(f"Rerun Result: status={m4.status}, input={m4.input_records_count}, valid={m4.valid_records_count}, "
              f"rejected={m4.rejected_records_count} (all duplicate keys correctly quarantined to prevent duplicate writes)")

    print("\n[PIPELINE] Execution completed successfully.")


if __name__ == "__main__":
    main()
