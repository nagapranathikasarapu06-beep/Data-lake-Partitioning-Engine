"""
Schema definitions, data types, validation models, and PyArrow schema specifications.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
import pyarrow as pa


# Valid currencies & status enums
VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD"}
VALID_TRANSACTION_STATUSES = {"COMPLETED", "FAILED", "PENDING", "REFUNDED"}
VALID_PAYMENT_METHODS = {"CREDIT_CARD", "DEBIT_CARD", "WIRE_TRANSFER", "ACH", "DIGITAL_WALLET"}
VALID_MERCHANT_STATUSES = {"ACTIVE", "SUSPENDED", "INACTIVE"}


class MerchantRecord(BaseModel):
    """Reference dimension entity representing an enrolled merchant."""
    merchant_id: str = Field(..., description="Unique merchant identifier, e.g. MERC_0001")
    merchant_name: str = Field(..., description="Legal business name")
    country_code: str = Field(..., description="ISO 3166-1 alpha-2 or alpha-3 code")
    status: str = Field("ACTIVE", description="Account status: ACTIVE, SUSPENDED, INACTIVE")
    created_at: str = Field(..., description="Merchant enrollment timestamp in ISO-8601 format")

    @field_validator("merchant_id")
    @classmethod
    def validate_merchant_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("merchant_id cannot be blank")
        return v.strip()

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        v_upper = v.upper()
        if v_upper not in VALID_MERCHANT_STATUSES:
            raise ValueError(f"status must be one of {VALID_MERCHANT_STATUSES}, got {v}")
        return v_upper


class RawTransaction(BaseModel):
    """Raw incoming event payload before enrichment and partition extraction."""
    transaction_id: str
    merchant_id: str
    customer_id: str
    amount: float
    currency: str
    status: str
    payment_method: str
    transaction_timestamp: str

    @field_validator("transaction_id")
    @classmethod
    def check_transaction_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("transaction_id cannot be empty or null")
        return v.strip()

    @field_validator("amount")
    @classmethod
    def check_amount(cls, v: float) -> float:
        if v is None or v <= 0:
            raise ValueError(f"amount must be strictly positive (> 0.0), got {v}")
        return round(float(v), 2)

    @field_validator("currency")
    @classmethod
    def check_currency(cls, v: str) -> str:
        v_upper = v.upper()
        if v_upper not in VALID_CURRENCIES:
            raise ValueError(f"currency must be one of {VALID_CURRENCIES}, got {v}")
        return v_upper

    @field_validator("status")
    @classmethod
    def check_status(cls, v: str) -> str:
        v_upper = v.upper()
        if v_upper not in VALID_TRANSACTION_STATUSES:
            raise ValueError(f"status must be one of {VALID_TRANSACTION_STATUSES}, got {v}")
        return v_upper

    @field_validator("payment_method")
    @classmethod
    def check_payment_method(cls, v: str) -> str:
        v_upper = v.upper()
        if v_upper not in VALID_PAYMENT_METHODS:
            raise ValueError(f"payment_method must be one of {VALID_PAYMENT_METHODS}, got {v}")
        return v_upper

    @field_validator("transaction_timestamp")
    @classmethod
    def check_timestamp(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("transaction_timestamp cannot be empty")
        # Validate parseability as ISO-8601
        try:
            # Handle trailing 'Z' or offset
            dt_str = v.replace("Z", "+00:00") if v.endswith("Z") else v
            datetime.fromisoformat(dt_str)
        except Exception as e:
            raise ValueError(f"transaction_timestamp must be valid ISO-8601, got '{v}': {e}")
        return v


class ProcessedTransaction(BaseModel):
    """Enriched, validated transaction ready for target lake storage."""
    transaction_id: str
    merchant_id: str
    customer_id: str
    amount: float
    currency: str
    status: str
    payment_method: str
    transaction_timestamp: str
    
    # Derived time partition attributes
    event_date: str          # YYYY-MM-DD (Hive partition key)
    event_year: int          # YYYY
    event_month: int         # MM
    
    # Lineage and governance metadata
    run_id: str
    source_file: str
    ingested_at: str


class QuarantineRecord(BaseModel):
    """Traceable quarantined record capturing rejected raw data and cause of rejection."""
    raw_record: str
    rejection_reason: str
    error_details: str
    run_id: str
    source_file: str
    rejected_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# PyArrow target schema for Silver lake Parquet storage
TARGET_PARQUET_SCHEMA = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("merchant_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("amount", pa.float64(), nullable=False),
    pa.field("currency", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("payment_method", pa.string(), nullable=False),
    pa.field("transaction_timestamp", pa.string(), nullable=False),
    pa.field("event_date", pa.string(), nullable=False),
    pa.field("event_year", pa.int32(), nullable=False),
    pa.field("event_month", pa.int32(), nullable=False),
    pa.field("run_id", pa.string(), nullable=False),
    pa.field("source_file", pa.string(), nullable=False),
    pa.field("ingested_at", pa.string(), nullable=False),
])

QUARANTINE_PARQUET_SCHEMA = pa.schema([
    pa.field("raw_record", pa.string(), nullable=False),
    pa.field("rejection_reason", pa.string(), nullable=False),
    pa.field("error_details", pa.string(), nullable=False),
    pa.field("run_id", pa.string(), nullable=False),
    pa.field("source_file", pa.string(), nullable=False),
    pa.field("rejected_at", pa.string(), nullable=False),
])
