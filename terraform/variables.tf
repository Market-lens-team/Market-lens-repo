# Store the GCP project ID
variable "project_id" {

  # Explain what this variable is used for
  description = "GCP project ID where MarketLens infrastructure will be created."

  # The project ID must be text
  type = string
}


# Store the default GCP region
variable "region" {

  # Explain what this variable is used for
  description = "Default GCP region for MarketLens resources."

  # The region must be text
  type = string

  # Use us-central1 if no other region is provided
  default = "us-central1"
}


# Store the GCS bucket location
variable "gcs_location" {

  # Explain what this variable is used for
  description = "Location of the MarketLens raw-data GCS bucket."

  # The location must be text
  type = string

  # Use the US multi-region by default
  default = "US"
}


# Store the name of the raw-data GCS bucket
variable "raw_bucket_name" {

  # Explain what this variable is used for
  description = "Globally unique name of the MarketLens raw-data GCS bucket."

  # The bucket name must be text
  type = string
}