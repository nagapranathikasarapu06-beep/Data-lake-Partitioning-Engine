"""
Command-Line Interface to execute Data Lake Pruning & File-Sizing Benchmarks.
Usage:
    python scripts/run_benchmark.py [--records 100000]
"""

import sys
import argparse
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.pipeline_config import DEFAULT_CONFIG
from benchmarks.benchmark_pruning_and_sizing import BenchmarkEngine


def main():
    parser = argparse.ArgumentParser(description="Data Lake Partitioning & File-Sizing Benchmark")
    parser.add_argument(
        "--records",
        type=int,
        default=100_000,
        help="Number of records to generate for the benchmark (default: 100,000)"
    )
    args = parser.parse_args()

    print(f"[BENCHMARK] Starting Benchmark with {args.records:,} records...")
    bench = BenchmarkEngine(config=DEFAULT_CONFIG, record_count=args.records)
    results = bench.run_all_benchmarks()

    print("\n[BENCHMARK] All benchmark queries executed successfully!")
    print(f"[BENCHMARK] Full analytical report available at: {results['report_path']}")


if __name__ == "__main__":
    main()
