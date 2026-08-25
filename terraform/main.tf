# ============================================================
# GOOGLE CLOUD PROVIDER CONFIGURATION
# ============================================================

# Configure the Google Cloud provider.
# Terraform will use this provider to create and manage
# resources inside Google Cloud.
provider "google" {

  # GCP project where all MarketLens resources will be created.
  # var.project_id will come from variables.tf / terraform.tfvars.
  project = var.project_id

  # Default GCP region for regional resources.
  # For your project we will use Mumbai region.
  region = var.region
}

# ============================================================
# GOOGLE CLOUD STORAGE - RAW DATA BUCKET
# ============================================================

# Create a Google Cloud Storage bucket for MarketLens raw data.
# This bucket will store the original CSV files without
# modifying their original structure.
resource "google_storage_bucket" "marketlens_raw" {

  # Use the bucket name defined in terraform.tfvars.
  # Bucket names must be globally unique across Google Cloud.
  name = var.bucket_name

  # Store the bucket in the Mumbai region.
  # This uses the region defined in terraform.tfvars.
  location = var.region

  # STANDARD storage class is suitable for frequently accessed
  # raw data during the ingestion process.
  storage_class = "STANDARD"

  # Enable Uniform Bucket-Level Access.
  # This makes IAM permissions consistent at the bucket level
  # instead of using separate object-level ACLs.
  uniform_bucket_level_access = true

  # Enable object versioning.
  # This keeps previous versions of objects when a file is
  # replaced or overwritten.
  versioning {
    enabled = true
  }
}

# ============================================================
# BIGQUERY - BRONZE DATASET
# ============================================================

# Create the Bronze dataset.
# Bronze contains the raw structured data after ingestion.
resource "google_bigquery_dataset" "bronze" {

  # GCP project where the dataset will be created.
  project = var.project_id

  # BigQuery dataset name.
  dataset_id = "bronze"

  # Store the dataset in the same region as our GCS bucket.
  location = var.region

  # Description of the purpose of this dataset.
  description = "Bronze layer containing raw structured MarketLens data."
}


# ============================================================
# BIGQUERY - SILVER DATASET
# ============================================================

# Create the Silver dataset.
# Silver contains cleaned, validated and standardized data.
resource "google_bigquery_dataset" "silver" {

  # GCP project where the dataset will be created.
  project = var.project_id

  # BigQuery dataset name.
  dataset_id = "silver"

  # Use the Mumbai region.
  location = var.region

  # Description of the purpose of this dataset.
  description = "Silver layer containing cleaned and validated MarketLens data."
}


# ============================================================
# BIGQUERY - GOLD DATASET
# ============================================================

# Create the Gold dataset.
# Gold contains business-ready analytical data and metrics.
resource "google_bigquery_dataset" "gold" {

  # GCP project where the dataset will be created.
  project = var.project_id

  # BigQuery dataset name.
  dataset_id = "gold"

  # Use the Mumbai region.
  location = var.region

  # Description of the purpose of this dataset.
  description = "Gold layer containing business-ready MarketLens analytical data."
}


# ============================================================
# BIGQUERY - QUARANTINED DATASET
# ============================================================

# Create the Quarantined dataset.
# Invalid or failed records will be stored here for investigation.
resource "google_bigquery_dataset" "quarantined" {

  # GCP project where the dataset will be created.
  project = var.project_id

  # BigQuery dataset name.
  dataset_id = "quarantined"

  # Use the Mumbai region.
  location = var.region

  # Description of the purpose of this dataset.
  description = "Quarantined data containing invalid or rejected MarketLens records."
}


# ============================================================
# BIGQUERY - SEMANTIC DATASET
# ============================================================

# Create the Semantic dataset.
# This layer will contain governed metrics and AI-facing data.
resource "google_bigquery_dataset" "semantic" {

  # GCP project where the dataset will be created.
  project = var.project_id

  # BigQuery dataset name.
  dataset_id = "semantic"

  # Use the Mumbai region.
  location = var.region

  # Description of the purpose of this dataset.
  description = "Semantic layer containing governed metrics for MarketLens and GenAI."
}




# ============================================================
# SERVICE ACCOUNT - DATA PIPELINE
# ============================================================

# Create a service account for the MarketLens data pipeline.
# This account will be used by ingestion, transformation,
# data quality, and orchestration components.
resource "google_service_account" "pipeline" {

  # The unique ID of the service account.
  # Google Cloud will use this value to create the account.
  account_id = "marketlens-pipeline-sa"

  # Human-readable name shown in Google Cloud Console.
  display_name = "MarketLens Data Pipeline"

  # Explain the purpose of this service account.
  description = "Service account for MarketLens data ingestion, transformation, and data quality."
}


# ============================================================
# SERVICE ACCOUNT - GENAI
# ============================================================

# Create a separate service account for the GenAI layer.
# Keeping it separate follows the principle of least privilege.
resource "google_service_account" "genai" {

  # Unique ID of the GenAI service account.
  account_id = "marketlens-genai-sa"

  # Human-readable name shown in Google Cloud Console.
  display_name = "MarketLens GenAI"

  # Explain the purpose of this service account.
  description = "Service account for MarketLens GenAI and semantic data access."
}


# ============================================================
# SERVICE ACCOUNT - API
# ============================================================

# Create a separate service account for the API/dashboard layer.
# This prevents the API from receiving unnecessary write permissions.
resource "google_service_account" "api" {

  # Unique ID of the API service account.
  account_id = "marketlens-api-sa"

  # Human-readable name shown in Google Cloud Console.
  display_name = "MarketLens API"

  # Explain the purpose of this service account.
  description = "Service account for MarketLens API and visualization access."
}



# ============================================================
# IAM PERMISSIONS
# ============================================================
#
# The permissions below follow the principle of least privilege:
#
# 1. Pipeline Service Account
#    - Can read/write raw GCS data.
#    - Can run BigQuery jobs.
#    - Can modify data in the required BigQuery datasets.
#    - Can write logs.
#
# 2. GenAI Service Account
#    - Can run BigQuery queries.
#    - Can READ Gold and Semantic data.
#    - Cannot modify Gold or Semantic data.
#
# 3. API Service Account
#    - Can run BigQuery queries.
#    - Can READ Gold and Semantic data.
#    - Cannot modify Gold or Semantic data.
#
# ============================================================



# ============================================================
# 1. PIPELINE SERVICE ACCOUNT → GCS
# ============================================================

# Give the pipeline service account permission to manage
# objects inside the raw GCS bucket.
resource "google_storage_bucket_iam_member" "pipeline_bucket" {

  # The GCS bucket where raw MarketLens files are stored.
  bucket = google_storage_bucket.marketlens_raw.name

  # Allows the pipeline to create, read, update and delete
  # objects inside this bucket.
  role = "roles/storage.objectAdmin"

  # Assign this permission to the pipeline service account.
  member = "serviceAccount:${google_service_account.pipeline.email}"
}



# ============================================================
# 2. PIPELINE SERVICE ACCOUNT → BIGQUERY JOBS
# ============================================================

# Allow the pipeline to execute BigQuery jobs such as
# queries, transformations and load jobs.
resource "google_project_iam_member" "pipeline_bigquery_job" {

  # GCP project where the permission is granted.
  project = var.project_id

  # Allows the service account to run BigQuery jobs.
  role = "roles/bigquery.jobUser"

  # Pipeline service account receiving the permission.
  member = "serviceAccount:${google_service_account.pipeline.email}"
}



# ============================================================
# 3. PIPELINE → BRONZE DATASET
# ============================================================

# Allow the pipeline to create and modify data in Bronze.
resource "google_bigquery_dataset_iam_member" "pipeline_bronze" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Bronze dataset created earlier.
  dataset_id = google_bigquery_dataset.bronze.dataset_id

  # Allows the pipeline to create, update and delete
  # BigQuery table data within the dataset.
  role = "roles/bigquery.dataEditor"

  # Pipeline service account.
  member = "serviceAccount:${google_service_account.pipeline.email}"
}



# ============================================================
# 4. PIPELINE → SILVER DATASET
# ============================================================

# Allow the pipeline to write cleaned and transformed
# data into the Silver layer.
resource "google_bigquery_dataset_iam_member" "pipeline_silver" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Silver dataset.
  dataset_id = google_bigquery_dataset.silver.dataset_id

  # Allows data modification in the Silver dataset.
  role = "roles/bigquery.dataEditor"

  # Pipeline service account.
  member = "serviceAccount:${google_service_account.pipeline.email}"
}



# ============================================================
# 5. PIPELINE → GOLD DATASET
# ============================================================

# Allow the pipeline to create and update
# business-ready analytical data.
resource "google_bigquery_dataset_iam_member" "pipeline_gold" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Gold dataset.
  dataset_id = google_bigquery_dataset.gold.dataset_id

  # Allows data modification in Gold.
  role = "roles/bigquery.dataEditor"

  # Pipeline service account.
  member = "serviceAccount:${google_service_account.pipeline.email}"
}



# ============================================================
# 6. PIPELINE → QUARANTINED DATASET
# ============================================================

# Allow the pipeline to store invalid or rejected records
# in the Quarantined dataset.
resource "google_bigquery_dataset_iam_member" "pipeline_quarantined" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Quarantined dataset.
  dataset_id = google_bigquery_dataset.quarantined.dataset_id

  # Allows the pipeline to write and manage
  # quarantined records.
  role = "roles/bigquery.dataEditor"

  # Pipeline service account.
  member = "serviceAccount:${google_service_account.pipeline.email}"
}



# ============================================================
# 7. PIPELINE → SEMANTIC DATASET
# ============================================================

# Allow the pipeline to create and update
# governed metrics and semantic-layer objects.
resource "google_bigquery_dataset_iam_member" "pipeline_semantic" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Semantic dataset.
  dataset_id = google_bigquery_dataset.semantic.dataset_id

  # Allows data modification in Semantic.
  role = "roles/bigquery.dataEditor"

  # Pipeline service account.
  member = "serviceAccount:${google_service_account.pipeline.email}"
}



# ============================================================
# 8. PIPELINE → CLOUD LOGGING
# ============================================================

# Allow the pipeline to write application and
# pipeline execution logs to Cloud Logging.
resource "google_project_iam_member" "pipeline_logging" {

  # GCP project where logs will be written.
  project = var.project_id

  # Permission to write log entries.
  role = "roles/logging.logWriter"

  # Pipeline service account.
  member = "serviceAccount:${google_service_account.pipeline.email}"
}



# ============================================================
# 9. GENAI → BIGQUERY JOBS
# ============================================================

# Allow the GenAI service account to execute
# BigQuery queries.
resource "google_project_iam_member" "genai_bigquery_job" {

  # GCP project where queries will run.
  project = var.project_id

  # Allows BigQuery job execution.
  role = "roles/bigquery.jobUser"

  # GenAI service account.
  member = "serviceAccount:${google_service_account.genai.email}"
}



# ============================================================
# 10. GENAI → GOLD DATASET
# ============================================================

# Give GenAI read-only access to business-ready
# analytical data.
resource "google_bigquery_dataset_iam_member" "genai_gold" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Gold dataset.
  dataset_id = google_bigquery_dataset.gold.dataset_id

  # Read-only access to data.
  role = "roles/bigquery.dataViewer"

  # GenAI service account.
  member = "serviceAccount:${google_service_account.genai.email}"
}



# ============================================================
# 11. GENAI → SEMANTIC DATASET
# ============================================================

# Give GenAI read-only access to governed metrics
# and semantic-layer data.
resource "google_bigquery_dataset_iam_member" "genai_semantic" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Semantic dataset.
  dataset_id = google_bigquery_dataset.semantic.dataset_id

  # Read-only access.
  role = "roles/bigquery.dataViewer"

  # GenAI service account.
  member = "serviceAccount:${google_service_account.genai.email}"
}



# ============================================================
# 12. API → BIGQUERY JOBS
# ============================================================

# Allow the API service account to execute
# BigQuery queries for dashboard/API requests.
resource "google_project_iam_member" "api_bigquery_job" {

  # GCP project where queries will run.
  project = var.project_id

  # Allows BigQuery job execution.
  role = "roles/bigquery.jobUser"

  # API service account.
  member = "serviceAccount:${google_service_account.api.email}"
}



# ============================================================
# 13. API → GOLD DATASET
# ============================================================

# Give the API read-only access to Gold data.
# The API does not need permission to modify analytical data.
resource "google_bigquery_dataset_iam_member" "api_gold" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Gold dataset.
  dataset_id = google_bigquery_dataset.gold.dataset_id

  # Read-only access.
  role = "roles/bigquery.dataViewer"

  # API service account.
  member = "serviceAccount:${google_service_account.api.email}"
}



# ============================================================
# 14. API → SEMANTIC DATASET
# ============================================================

# Give the API read-only access to governed
# semantic-layer data.
resource "google_bigquery_dataset_iam_member" "api_semantic" {

  # GCP project containing the dataset.
  project = var.project_id

  # Reference the Semantic dataset.
  dataset_id = google_bigquery_dataset.semantic.dataset_id

  # Read-only access.
  role = "roles/bigquery.dataViewer"

  # API service account.
  member = "serviceAccount:${google_service_account.api.email}"
}