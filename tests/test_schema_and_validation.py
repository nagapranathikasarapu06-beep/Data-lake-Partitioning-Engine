"""
Automated Tests: Schema Constraints, Business Rules, and Validation.
"""

import unittest
from src.validator import DataValidator
from src.schema import RawTransaction


class TestSchemaAndValidation(unittest.TestCase):
    def setUp(self):
        self.active_merchants = {"MERC_0001", "MERC_0002"}
        self.validator = DataValidator(valid_merchant_ids=self.active_merchants)

    def test_valid_record(self):
        records = [
            {
                "transaction_id": "TX_VALID_1",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_1",
                "amount": 100.0,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            }
        ]
        valid, quarantined = self.validator.validate_batch(records, "run_1", "src.json")
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(quarantined), 0)

    def test_missing_required_field(self):
        records = [
            {
                "transaction_id": "TX_MISSING_AMT",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_1",
                # "amount" missing
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            }
        ]
        valid, quarantined = self.validator.validate_batch(records, "run_1", "src.json")
        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0].rejection_reason, "MISSING_REQUIRED_FIELD")

    def test_negative_and_zero_amount(self):
        records = [
            {
                "transaction_id": "TX_NEG",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_1",
                "amount": -25.50,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            },
            {
                "transaction_id": "TX_ZERO",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_2",
                "amount": 0.0,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            }
        ]
        valid, quarantined = self.validator.validate_batch(records, "run_1", "src.json")
        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 2)
        self.assertEqual(quarantined[0].rejection_reason, "INVALID_AMOUNT")
        self.assertEqual(quarantined[1].rejection_reason, "INVALID_AMOUNT")

    def test_invalid_currency(self):
        records = [
            {
                "transaction_id": "TX_CURR",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_1",
                "amount": 50.0,
                "currency": "DOGECOIN",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            }
        ]
        valid, quarantined = self.validator.validate_batch(records, "run_1", "src.json")
        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0].rejection_reason, "INVALID_CURRENCY")

    def test_referential_integrity_violation(self):
        records = [
            {
                "transaction_id": "TX_REF_VIOLATION",
                "merchant_id": "MERC_UNKNOWN_9999",
                "customer_id": "CUST_1",
                "amount": 80.0,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026-11-10T10:00:00Z"
            }
        ]
        valid, quarantined = self.validator.validate_batch(records, "run_1", "src.json")
        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0].rejection_reason, "FOREIGN_KEY_VIOLATION")

    def test_malformed_timestamp(self):
        records = [
            {
                "transaction_id": "TX_BAD_TIME",
                "merchant_id": "MERC_0001",
                "customer_id": "CUST_1",
                "amount": 90.0,
                "currency": "USD",
                "status": "COMPLETED",
                "payment_method": "CREDIT_CARD",
                "transaction_timestamp": "2026/11/10 10:00 AM (NOT ISO)"
            }
        ]
        valid, quarantined = self.validator.validate_batch(records, "run_1", "src.json")
        self.assertEqual(len(valid), 0)
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0].rejection_reason, "INVALID_TIMESTAMP")


if __name__ == "__main__":
    unittest.main()
