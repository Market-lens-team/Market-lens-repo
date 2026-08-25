# ============================================================
# TERRAFORM OUTPUTS
# ============================================================
#
# Outputs display important information after Terraform
# creates the infrastructure.
#
# These values are also useful for other team members,
# CI/CD pipelines, and future Terraform configurations.
# ============================================================


# ============================================================
# GCS RAW BUCKET
# ============================================================

# Display the name of the raw GCS bucket.
output "raw_bucket_name" {

  # Return the actual name of the bucket created by Terraform.
  value = google_storage_bucket.marketlens_raw.name

  # Explain what this output represents.
  description = "Name of the MarketLens raw GCS bucket."
}


# ============================================================
# GCS RAW BUCKET URL
# ============================================================

# Display the GCS bucket URL.
output "raw_bucket_url" {

  # Return the URL of the created bucket.
  value = google_storage_bucket.marketlens_raw.url

  # Explain what this output represents.
  description = "URL of the MarketLens raw GCS bucket."
}


# ============================================================
# BIGQUERY DATASETS
# ============================================================

# Display the Bronze dataset ID.
output "bronze_dataset_id" {

  # Return the Bronze dataset ID.
  value = google_bigquery_dataset.bronze.dataset_id

  # Description of the output.
  description = "BigQuery Bronze dataset ID."
}


# Display the Silver dataset ID.
output "silver_dataset_id" {

  # Return the Silver dataset ID.
  value = google_bigquery_dataset.silver.dataset_id

  # Description of the output.
  description = "BigQuery Silver dataset ID."
}


# Display the Gold dataset ID.
output "gold_dataset_id" {

  # Return the Gold dataset ID.
  value = google_bigquery_dataset.gold.dataset_id

  # Description of the output.
  description = "BigQuery Gold dataset ID."
}


# Display the Quarantined dataset ID.
output "quarantined_dataset_id" {

  # Return the Quarantined dataset ID.
  value = google_bigquery_dataset.quarantined.dataset_id

  # Description of the output.
  description = "BigQuery Quarantined dataset ID."
}


# Display the Semantic dataset ID.
output "semantic_dataset_id" {

  # Return the Semantic dataset ID.
  value = google_bigquery_dataset.semantic.dataset_id

  # Description of the output.
  description = "BigQuery Semantic dataset ID."
}


# ============================================================
# SERVICE ACCOUNTS
# ============================================================

# Display the email address of the Pipeline service account.
output "pipeline_service_account" {

  # Return the Pipeline service account email.
  value = google_service_account.pipeline.email

  # Description of the output.
  description = "Email address of the MarketLens Pipeline service account."
}


# Display the email address of the GenAI service account.
output "genai_service_account" {

  # Return the GenAI service account email.
  value = google_service_account.genai.email

  # Description of the output.
  description = "Email address of the MarketLens GenAI service account."
}


# Display the email address of the API service account.
output "api_service_account" {

  # Return the API service account email.
  value = google_service_account.api.email

  # Description of the output.
  description = "Email address of the MarketLens API service account."
}