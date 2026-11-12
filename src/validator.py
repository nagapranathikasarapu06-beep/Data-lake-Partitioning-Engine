"""
Data Validation and Quality Assurance Engine.
Performs schema validation, business rule checks, referential integrity verification,
duplicate detection, and routes invalid records to quarantine with full traceability.
"""

import json
from datetime import datetime
from typing import List, Dict, Any, Set, Tuple
from src.schema import (
    RawTransaction,
    QuarantineRecord,
    VALID_CURRENCIES,
    VALID_TRANSACTION_STATUSES,
    VALID_PAYMENT_METHODS,
)


class DataValidator:
    """
    Validates records against schema constraints, business rules, referential integrity,
    and handles deduplication.
    """

    def __init__(self, valid_merchant_ids: Set[str]):
        """
        Args:
            valid_merchant_ids: Set of active merchant IDs for referential integrity checks.
        """
        self.valid_merchant_ids = valid_merchant_ids

    def validate_batch(
        self,
        records: List[Dict[str, Any]],
        run_id: str,
        source_file: str,
        existing_seen_ids: Set[str] = None
    ) -> Tuple[List[RawTransaction], List[QuarantineRecord]]:
        """
        Validates a raw batch of records.

        Returns:
            Tuple of (valid_records, quarantined_records)
        """
        valid_records: List[RawTransaction] = []
        quarantined: List[QuarantineRecord] = []
        seen_in_batch: Set[str] = set()
        seen_global = existing_seen_ids or set()

        for idx, raw in enumerate(records):
            raw_str = json.dumps(raw) if isinstance(raw, dict) else str(raw)

            # Check 1: Record format
            if not isinstance(raw, dict):
                quarantined.append(
                    QuarantineRecord(
                        raw_record=raw_str,
                        rejection_reason="MALFORMED_RECORD",
                        error_details="Record is not a valid JSON object/dictionary",
                        run_id=run_id,
                        source_file=source_file
                    )
                )
                continue

            # Check 2: Required fields presence
            required_keys = [
                "transaction_id",
                "merchant_id",
                "customer_id",
                "amount",
                "currency",
                "status",
                "payment_method",
                "transaction_timestamp",
            ]
            missing_keys = [k for k in required_keys if k not in raw or raw[k] is None]
            if missing_keys:
                quarantined.append(
                    QuarantineRecord(
                        raw_record=raw_str,
                        rejection_reason="MISSING_REQUIRED_FIELD",
                        error_details=f"Missing required fields: {missing_keys}",
                        run_id=run_id,
                        source_file=source_file
                    )
                )
                continue

            # Check 3: Schema & Type Validation via Pydantic
            try:
                tx = RawTransaction(**raw)
            except Exception as e:
                # Classify specific failure reason
                err_msg = str(e)
                if "amount" in err_msg:
                    reason = "INVALID_AMOUNT"
                elif "currency" in err_msg:
                    reason = "INVALID_CURRENCY"
                elif "timestamp" in err_msg:
                    reason = "INVALID_TIMESTAMP"
                elif "status" in err_msg or "payment_method" in err_msg:
                    reason = "BUSINESS_RULE_VIOLATION"
                else:
                    reason = "SCHEMA_VALIDATION_ERROR"

                quarantined.append(
                    QuarantineRecord(
                        raw_record=raw_str,
                        rejection_reason=reason,
                        error_details=err_msg,
                        run_id=run_id,
                        source_file=source_file
                    )
                )
                continue

            # Check 4: Referential Integrity
            if tx.merchant_id not in self.valid_merchant_ids:
                quarantined.append(
                    QuarantineRecord(
                        raw_record=raw_str,
                        rejection_reason="FOREIGN_KEY_VIOLATION",
                        error_details=f"Merchant '{tx.merchant_id}' does not exist in active reference directory",
                        run_id=run_id,
                        source_file=source_file
                    )
                )
                continue

            # Check 5: Duplicate Key Detection (within batch & across existing)
            if tx.transaction_id in seen_in_batch or tx.transaction_id in seen_global:
                quarantined.append(
                    QuarantineRecord(
                        raw_record=raw_str,
                        rejection_reason="DUPLICATE_KEY",
                        error_details=f"Duplicate transaction_id '{tx.transaction_id}' detected",
                        run_id=run_id,
                        source_file=source_file
                    )
                )
                continue

            # If all checks pass
            seen_in_batch.add(tx.transaction_id)
            seen_global.add(tx.transaction_id)
            valid_records.append(tx)

        return valid_records, quarantined
