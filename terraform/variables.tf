# ============================================================
# PROJECT ID
# ============================================================

# GCP Project ID where MarketLens infrastructure will be created.
variable "project_id" {

  # The project ID must be provided as a string.
  type = string

  # Description of this variable.
  description = "The GCP project ID for the MarketLens project."
}


# ============================================================
# REGION
# ============================================================

# Default GCP region for MarketLens resources.
variable "region" {

  # The region value must be a string.
  type = string

  # Mumbai region.
  # We will use this as the default value.
  default = "asia-south1"

  # Description of this variable.
  description = "The GCP region where MarketLens resources will be created."
}


# ============================================================
# RAW GCS BUCKET NAME
# ============================================================

# Name of the Google Cloud Storage bucket
# that will store the original/raw MarketLens files.
variable "bucket_name" {

  # Bucket name must be a string.
  type = string

  # Description of this variable.
  description = "Globally unique GCS bucket name for MarketLens raw data."
}