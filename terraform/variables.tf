variable "project_id" {
  description = "GCP project ID where MarketLens infrastructure will be created."
  type        = string
}

variable "region" {
  description = "Default GCP region for MarketLens resources."
  type        = string
  default     = "us-central1"
}

variable "gcs_location" {
  description = "Location of the MarketLens raw-data GCS bucket."
  type        = string
  default     = "US"
}

variable "raw_bucket_name" {
  description = "Globally unique name of the MarketLens raw-data GCS bucket."
  type        = string
}