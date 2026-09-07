"""
config.py

All the fixed settings for the GCS -> Bronze loader.
"""

# ---- GCP / BQ settings ----
PROJECT_ID = "market-lens-506611"
BUCKET_NAME = "market-lens-506611-raw-mlteam-2026"

BRONZE_DATASET = "bronze"
STOCK_TABLE = f"{PROJECT_ID}.{BRONZE_DATASET}.bronze_stock_prices"
ETF_TABLE = f"{PROJECT_ID}.{BRONZE_DATASET}.bronze_etf_prices"
METADATA_TABLE = f"{PROJECT_ID}.{BRONZE_DATASET}.bronze_symbol_metadata"

AUDIT_TABLE = f"{PROJECT_ID}.{BRONZE_DATASET}.ingestion_audit"

# ---- batching ----
STOCK_BATCH_SIZE = 500
ETF_BATCH_SIZE = 500

# ---- asset types this pipeline knows how to process ----
# each _READY trigger processes ALL of these, one after another
ASSET_TYPES = ["stocks", "etfs"]

# name of the metadata file, expected directly under the load_type folder
# e.g. historical/symbols_valid_meta.csv
METADATA_FILENAME = "symbols_valid_meta.csv"

# the exact filename that triggers processing - anything else is ignored
READY_MARKER_FILENAME = "_READY"