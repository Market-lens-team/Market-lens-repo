import os


PROJECT_ID = os.getenv(
    "PROJECT_ID",
    "market-lens-506611",
)


REGION = os.getenv(
    "REGION",
    "us-central1",
)


BUCKET_NAME = os.getenv(
    "BUCKET_NAME",
    "market-lens-506611-raw-mlteam-2026",
)


BRONZE_DATASET = "bronze"


STOCK_TABLE = (
    "bronze_stock_prices"
)


ETF_TABLE = (
    "bronze_etf_prices"
)


METADATA_TABLE = (
    "bronze_symbol_metadata"
)


AUDIT_TABLE = (
    "ingestion_audit"
)


QUARANTINE_PREFIX = (
    "quarantine"
)