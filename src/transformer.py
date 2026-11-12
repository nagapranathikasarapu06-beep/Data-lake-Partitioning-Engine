"""
Data Transformation and Partition Derivation Module.
Enriches validated records with event-time partition columns (event_date, event_year, event_month)
and audit metadata (run_id, source_file, ingested_at).
"""

from datetime import datetime, timezone
from typing import List
import pyarrow as pa
from src.schema import RawTransaction, ProcessedTransaction, TARGET_PARQUET_SCHEMA


class DataTransformer:
    """Transforms validated raw transactions into partitioned silver-ready structures."""

    @staticmethod
    def transform_batch(
        valid_records: List[RawTransaction],
        run_id: str,
        source_file: str
    ) -> List[ProcessedTransaction]:
        """
        Derives partition keys and audit columns deterministically.
        """
        now_utc = datetime.now(timezone.utc).isoformat()
        processed: List[ProcessedTransaction] = []

        for tx in valid_records:
            # Parse timestamp to extract event date attributes
            dt_str = tx.transaction_timestamp.replace("Z", "+00:00") if tx.transaction_timestamp.endswith("Z") else tx.transaction_timestamp
            dt = datetime.fromisoformat(dt_str)

            event_date = dt.strftime("%Y-%m-%d")
            event_year = dt.year
            event_month = dt.month

            item = ProcessedTransaction(
                transaction_id=tx.transaction_id,
                merchant_id=tx.merchant_id,
                customer_id=tx.customer_id,
                amount=tx.amount,
                currency=tx.currency,
                status=tx.status,
                payment_method=tx.payment_method,
                transaction_timestamp=tx.transaction_timestamp,
                event_date=event_date,
                event_year=event_year,
                event_month=event_month,
                run_id=run_id,
                source_file=source_file,
                ingested_at=now_utc,
            )
            processed.append(item)

        return processed

    @staticmethod
    def to_arrow_table(processed_records: List[ProcessedTransaction]) -> pa.Table:
        """Converts processed records into a PyArrow Table matching TARGET_PARQUET_SCHEMA."""
        if not processed_records:
            return pa.Table.from_arrays(
                [pa.array([], type=f.type) for f in TARGET_PARQUET_SCHEMA],
                schema=TARGET_PARQUET_SCHEMA
            )

        data = {
            "transaction_id": [r.transaction_id for r in processed_records],
            "merchant_id": [r.merchant_id for r in processed_records],
            "customer_id": [r.customer_id for r in processed_records],
            "amount": [r.amount for r in processed_records],
            "currency": [r.currency for r in processed_records],
            "status": [r.status for r in processed_records],
            "payment_method": [r.payment_method for r in processed_records],
            "transaction_timestamp": [r.transaction_timestamp for r in processed_records],
            "event_date": [r.event_date for r in processed_records],
            "event_year": [r.event_year for r in processed_records],
            "event_month": [r.event_month for r in processed_records],
            "run_id": [r.run_id for r in processed_records],
            "source_file": [r.source_file for r in processed_records],
            "ingested_at": [r.ingested_at for r in processed_records],
        }

        return pa.Table.from_pydict(data, schema=TARGET_PARQUET_SCHEMA)
