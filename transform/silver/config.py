"""
config.py

Configuration settings for Bronze -> Silver pipeline.
"""

# -------------------------
# GCP Configuration
# -------------------------

PROJECT_ID = "market-lens-506611"

REGION = "us-central1"

BUCKET_NAME = "market-lens-506611-raw-mlteam-2026"


# -------------------------
# BigQuery Datasets
# -------------------------

BRONZE_DATASET = "bronze"

SILVER_DATASET = "silver"


# -------------------------
# Bronze Source Tables
# -------------------------

STOCK_TABLE = "bronze_stock_prices"

ETF_TABLE = "bronze_etf_prices"

METADATA_TABLE = "bronze_symbol_metadata"


# -------------------------
# Silver Target Tables
# -------------------------

SILVER_TABLE = "silver_market_data"

SILVER_METADATA_TABLE = "silver_symbol_metadata"


# -------------------------
# Audit
# -------------------------

AUDIT_DATASET = "bronze"

AUDIT_TABLE = "ingestion_audit"


# -------------------------
# Trigger
# -------------------------

SILVER_TRIGGER_PREFIX = "silver_trigger/"

SILVER_TRIGGER_SUFFIX = "_ready"


# -------------------------
# Error Handling
# -------------------------

QUARANTINE_PREFIX = "quarantine"
QUARANTINE_DATASET = "quarantined"