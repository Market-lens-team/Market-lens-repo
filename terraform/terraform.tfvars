# ============================================================
# MARKETLENS GCP PROJECT
# ============================================================

# Your Google Cloud Project ID.
project_id = "market-lens-506605"


# ============================================================
# GCP REGION
# ============================================================

# Mumbai region.
# All regional MarketLens resources will use this region
# unless a resource explicitly specifies another location.
region = "asia-south1"


# ============================================================
# RAW DATA GCS BUCKET
# ============================================================

# GCS bucket used to store the original/raw MarketLens data.
#
# Bucket names must be globally unique.
# Using the project ID as part of the name helps maintain uniqueness.
bucket_name = "market-lens-506605-raw"