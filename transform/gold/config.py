"""
config.py
Configuration settings for Silver -> Gold pipeline.
"""

PROJECT_ID = "market-lens-506611"
REGION = "us-central1"

GOLD_DATASET = "gold"

AUDIT_DATASET = "bronze"
AUDIT_TABLE = "ingestion_audit"

GOLD_TRIGGER_PREFIX = "gold_trigger/"
GOLD_TRIGGER_SUFFIX = "_ready"
