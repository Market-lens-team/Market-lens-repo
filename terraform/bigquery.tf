# ============================================================
# 1. BRONZE LAYER
# Raw structured data from GCS
# ============================================================

resource "google_bigquery_dataset" "bronze" {
  project = var.project_id

  # BigQuery dataset ID
  dataset_id = "bronze"

  # Human-readable name
  friendly_name = "MarketLens Bronze"

  # Purpose of this layer
  description = "Raw structured stock and ETF market data."

  # Single GCP region
  location = var.region
}


# ============================================================
# 2. SILVER LAYER
# Cleaned and validated data
# ============================================================

resource "google_bigquery_dataset" "silver" {
  project = var.project_id

  dataset_id = "silver"

  friendly_name = "MarketLens Silver"

  description = "Cleaned and validated market data."

  location = var.region
}


# ============================================================
# 3. GOLD LAYER
# Analytics-ready and derived data
# ============================================================

resource "google_bigquery_dataset" "gold" {
  project = var.project_id

  dataset_id = "gold"

  friendly_name = "MarketLens Gold"

  description = "Analytics-ready and derived market data."

  location = var.region
}


# ============================================================
# 4. QUARANTINED LAYER
# Invalid or rejected records
# ============================================================

resource "google_bigquery_dataset" "quarantined" {
  project = var.project_id

  dataset_id = "quarantined"

  friendly_name = "MarketLens Quarantined"

  description = "Invalid and rejected records from data validation."

  location = var.region
}


# ============================================================
# 5. SEMANTIC LAYER
# Business-ready data for API, dashboard and GenAI
# ============================================================

resource "google_bigquery_dataset" "semantic" {
  project = var.project_id

  dataset_id = "semantic"

  friendly_name = "MarketLens Semantic"

  description = "Business-ready data for analytics, APIs, dashboards and GenAI."

  location = var.region
}