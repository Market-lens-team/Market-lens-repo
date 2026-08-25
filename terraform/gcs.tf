resource "google_storage_bucket" "raw_data" {
  name     = var.raw_bucket_name
  project  = var.project_id
  location = var.region

  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  force_destroy = false
}