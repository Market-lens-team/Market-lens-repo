# ============================================================
# CLOUD FUNCTION SERVICE ACCOUNT
# ============================================================
# This service account will be used by the Cloud Function
# to access the GCP resources required by the application.
# ============================================================

resource "google_service_account" "cloud_function" {
  project = var.project_id

  # Unique service account ID.
  account_id = "marketlens-cloud-function-sa"

  # Human-readable display name.
  display_name = "MarketLens Cloud Function Service Account"

  # Description of the service account's purpose.
  description = "Service account used by MarketLens Cloud Functions."
}