"""
Dataset Generator Module.
Generates reproducible synthetic datasets with fixed random seeds for:
1. Reference merchants dimension.
2. Starter sample batch (for quick review).
3. Explicit negative edge-cases batch.
4. Late-arriving / out-of-order data batch.
5. Large-scale configurable volume generator (10,000 - 100,000+ records).
Keeps expected ground truth manifests separate from input datasets.
"""

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

from config.pipeline_config import PipelineConfig, DEFAULT_CONFIG


class DatasetGenerator:
    """Deterministic synthetic data generator for data lake partitioning experiments."""

    def __init__(self, seed: int = 42, config: Optional[PipelineConfig] = None):
        self.seed = seed
        self.rng = random.Random(seed)
        self.config = config or DEFAULT_CONFIG
        self.config.ensure_directories()

        self.currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]
        self.statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "FAILED"]
        self.payment_methods = ["CREDIT_CARD", "DEBIT_CARD", "WIRE_TRANSFER", "ACH", "DIGITAL_WALLET"]

    def generate_reference_merchants(self, num_merchants: int = 20) -> List[Dict[str, Any]]:
        """Generates reference merchant dimension records."""
        merchants = []
        countries = ["US", "GB", "DE", "FR", "CA", "JP", "AU"]

        for i in range(1, num_merchants + 1):
            merchant_id = f"MERC_{i:04d}"
            # Make 2 merchants suspended to test referential integrity filtering if needed
            status = "SUSPENDED" if i in (18, 19) else "ACTIVE"
            merchants.append({
                "merchant_id": merchant_id,
                "merchant_name": f"Global Retail Partner {i}",
                "country_code": self.rng.choice(countries),
                "status": status,
                "created_at": "2025-01-01T00:00:00Z"
            })

        out_path = self.config.REFERENCE_DIR / "merchants.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(merchants, f, indent=2)

        return merchants

    def generate_transaction_records(
        self,
        count: int,
        start_date: datetime,
        days_span: int,
        merchant_ids: List[str],
        inject_duplicates_ratio: float = 0.02,
        inject_invalid_ratio: float = 0.03,
        batch_prefix: str = "TX",
    ) -> Dict[str, Any]:
        """
        Generates normal, boundary, late-arriving, and deliberate invalid records.
        Returns a dict with 'records' and a separate 'ground_truth_manifest'.
        """
        records: List[Dict[str, Any]] = []
        valid_count = 0
        duplicate_count = 0
        invalid_count = 0
        seen_keys = []

        for i in range(count):
            # Check if injecting intentional defect
            roll = self.rng.random()
            
            # Timestamp generation across the date span
            offset_seconds = self.rng.randint(0, max(1, days_span * 86400))
            record_time = start_date + timedelta(seconds=offset_seconds)
            time_iso = record_time.strftime("%Y-%m-%dT%H:%M:%SZ")

            if roll < inject_invalid_ratio:
                # Deliberate invalid records
                invalid_type = self.rng.choice([
                    "negative_amount",
                    "invalid_currency",
                    "invalid_merchant",
                    "missing_field",
                    "invalid_timestamp"
                ])
                invalid_count += 1
                if invalid_type == "negative_amount":
                    rec = {
                        "transaction_id": f"{batch_prefix}_INV_{uuid.uuid4().hex[:12]}",
                        "merchant_id": self.rng.choice(merchant_ids),
                        "customer_id": f"CUST_{self.rng.randint(1000, 9999)}",
                        "amount": -round(self.rng.uniform(1.0, 500.0), 2),
                        "currency": "USD",
                        "status": "COMPLETED",
                        "payment_method": "CREDIT_CARD",
                        "transaction_timestamp": time_iso,
                    }
                elif invalid_type == "invalid_currency":
                    rec = {
                        "transaction_id": f"{batch_prefix}_INV_{uuid.uuid4().hex[:12]}",
                        "merchant_id": self.rng.choice(merchant_ids),
                        "customer_id": f"CUST_{self.rng.randint(1000, 9999)}",
                        "amount": round(self.rng.uniform(10.0, 100.0), 2),
                        "currency": "FAKE_CURRENCY",
                        "status": "COMPLETED",
                        "payment_method": "CREDIT_CARD",
                        "transaction_timestamp": time_iso,
                    }
                elif invalid_type == "invalid_merchant":
                    rec = {
                        "transaction_id": f"{batch_prefix}_INV_{uuid.uuid4().hex[:12]}",
                        "merchant_id": "MERC_DOES_NOT_EXIST_9999",
                        "customer_id": f"CUST_{self.rng.randint(1000, 9999)}",
                        "amount": round(self.rng.uniform(10.0, 100.0), 2),
                        "currency": "USD",
                        "status": "COMPLETED",
                        "payment_method": "CREDIT_CARD",
                        "transaction_timestamp": time_iso,
                    }
                elif invalid_type == "missing_field":
                    rec = {
                        "transaction_id": f"{batch_prefix}_INV_{uuid.uuid4().hex[:12]}",
                        # merchant_id deliberately omitted
                        "customer_id": f"CUST_{self.rng.randint(1000, 9999)}",
                        "amount": round(self.rng.uniform(10.0, 100.0), 2),
                        "currency": "USD",
                        "status": "COMPLETED",
                        "payment_method": "CREDIT_CARD",
                        "transaction_timestamp": time_iso,
                    }
                else:
                    rec = {
                        "transaction_id": f"{batch_prefix}_INV_{uuid.uuid4().hex[:12]}",
                        "merchant_id": self.rng.choice(merchant_ids),
                        "customer_id": f"CUST_{self.rng.randint(1000, 9999)}",
                        "amount": round(self.rng.uniform(10.0, 100.0), 2),
                        "currency": "USD",
                        "status": "COMPLETED",
                        "payment_method": "CREDIT_CARD",
                        "transaction_timestamp": "NOT_A_TIMESTAMP_CORRUPT",
                    }
                records.append(rec)

            elif roll < (inject_invalid_ratio + inject_duplicates_ratio) and seen_keys:
                # Injected duplicate record (reuse an existing transaction_id)
                duplicate_count += 1
                dup_id = self.rng.choice(seen_keys)
                rec = {
                    "transaction_id": dup_id,
                    "merchant_id": self.rng.choice(merchant_ids),
                    "customer_id": f"CUST_{self.rng.randint(1000, 9999)}",
                    "amount": round(self.rng.uniform(5.0, 500.0), 2),
                    "currency": self.rng.choice(self.currencies),
                    "status": self.rng.choice(self.statuses),
                    "payment_method": self.rng.choice(self.payment_methods),
                    "transaction_timestamp": time_iso,
                }
                records.append(rec)

            else:
                # Valid normal or boundary transaction
                tx_id = f"{batch_prefix}_{self.seed}_{i:08d}"
                seen_keys.append(tx_id)
                valid_count += 1
                
                # Boundary value handling: small cents or large transaction
                if i % 500 == 0:
                    amount = 0.01  # Minimum valid amount
                elif i % 500 == 1:
                    amount = 999_999.99  # Large amount
                else:
                    amount = round(self.rng.uniform(2.50, 1500.00), 2)

                rec = {
                    "transaction_id": tx_id,
                    "merchant_id": self.rng.choice(merchant_ids),
                    "customer_id": f"CUST_{self.rng.randint(10000, 99999)}",
                    "amount": amount,
                    "currency": self.rng.choice(self.currencies),
                    "status": self.rng.choice(self.statuses),
                    "payment_method": self.rng.choice(self.payment_methods),
                    "transaction_timestamp": time_iso,
                }
                records.append(rec)

        manifest = {
            "total_generated": len(records),
            "expected_valid": valid_count,
            "expected_duplicates": duplicate_count,
            "expected_invalid": invalid_count,
            "random_seed": self.seed,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "days_span": days_span,
        }

        return {"records": records, "manifest": manifest}

    def generate_all_sample_batches(self):
        """Generates starter, edge cases, late-arriving, and benchmark samples."""
        merchants = self.generate_reference_merchants(num_merchants=20)
        active_ids = [m["merchant_id"] for m in merchants if m["status"] == "ACTIVE"]

        base_date = datetime(2026, 11, 1, 0, 0, 0, tzinfo=timezone.utc)

        # 1. Starter batch (500 records across 5 days)
        starter_data = self.generate_transaction_records(
            count=500,
            start_date=base_date,
            days_span=5,
            merchant_ids=active_ids,
            inject_duplicates_ratio=0.01,
            inject_invalid_ratio=0.02,
            batch_prefix="TX_START"
        )
        starter_path = self.config.SAMPLE_INPUTS_DIR / "starter_batch.json"
        starter_manifest_path = self.config.SAMPLE_INPUTS_DIR / "starter_batch_manifest.json"
        with open(starter_path, "w", encoding="utf-8") as f:
            json.dump(starter_data["records"], f, indent=2)
        with open(starter_manifest_path, "w", encoding="utf-8") as f:
            json.dump(starter_data["manifest"], f, indent=2)

        # 2. Edge-cases negative batch (explicit malformed, nulls, duplicates)
        edge_data = [
            # 1. Normal valid record
            {
                "transaction_id": "TX_EDGE_001",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_9999",
                "amount": 100.50,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-05T12:00:00Z"
            },
            # 2. Duplicate key (exact same ID as above)
            {
                "transaction_id": "TX_EDGE_001",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_9999",
                "amount": 100.50,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-05T12:00:00Z"
            },
            # 3. Missing required field (amount is None)
            {
                "transaction_id": "TX_EDGE_002",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_9999",
                "amount": None,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-05T12:05:00Z"
            },
            # 4. Negative amount
            {
                "transaction_id": "TX_EDGE_003",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_9999",
                "amount": -50.00,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-05T12:10:00Z"
            },
            # 5. Invalid foreign key (merchant not registered)
            {
                "transaction_id": "TX_EDGE_004",
                "merchant_id": "MERC_NON_EXISTENT",
                "customer_id": "CUST_9999",
                "amount": 25.00,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-05T12:15:00Z"
            },
            # 6. Invalid currency
            {
                "transaction_id": "TX_EDGE_005",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_9999",
                "amount": 42.00,
                "currency": "BITCOIN",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-05T12:20:00Z"
            },
            # 7. Malformed timestamp
            {
                "transaction_id": "TX_EDGE_006",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_9999",
                "amount": 10.00,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "INVALID-DATE-STRING"
            },
            # 8. Boundary minimum amount valid
            {
                "transaction_id": "TX_EDGE_007",
                "merchant_id": "MERC_0002",
                "customer_id": "CUST_8888",
                "amount": 0.01,
                "currency": "EUR",
                "status": "COMPLETED",
                "payment_method": "DEBIT_CARD",
                "transaction_timestamp": "2026-11-05T12:30:00Z"
            }
        ]
        edge_path = self.config.SAMPLE_INPUTS_DIR / "edge_cases_batch.json"
        with open(edge_path, "w", encoding="utf-8") as f:
            json.dump(edge_data, f, indent=2)

        # 3. Late-arriving batch (events stamped from 2026-10-15 to 2026-10-20 arriving in Nov)
        late_base = datetime(2026, 10, 15, 0, 0, 0, tzinfo=timezone.utc)
        late_data = self.generate_transaction_records(
            count=150,
            start_date=late_base,
            days_span=5,
            merchant_ids=active_ids,
            inject_duplicates_ratio=0.0,
            inject_invalid_ratio=0.0,
            batch_prefix="TX_LATE"
        )
        late_path = self.config.SAMPLE_INPUTS_DIR / "late_arriving_batch.json"
        with open(late_path, "w", encoding="utf-8") as f:
            json.dump(late_data["records"], f, indent=2)

        return {
            "starter_batch": starter_path,
            "edge_cases_batch": edge_path,
            "late_arriving_batch": late_path,
            "merchants": self.config.REFERENCE_DIR / "merchants.json"
        }
