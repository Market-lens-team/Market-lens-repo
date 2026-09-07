# Create a Google Cloud Storage bucket
resource "google_storage_bucket" "raw_data" {

  # Set the bucket name using the variable
  name = var.raw_bucket_name

  # Create the bucket inside the given GCP project
  project = var.project_id

  # Set the bucket's location/region
  location = var.region

  # Use Standard storage because the raw data is accessed frequently
  storage_class = "STANDARD"

  # Use IAM permissions at bucket level instead of object-level permissions
  uniform_bucket_level_access = true

  # Prevent anyone from making the bucket publicly accessible
  public_access_prevention = "enforced"

  # Keep older versions of files when they are replaced or deleted
  versioning {
    enabled = true
  }

  # Do not automatically delete all files when destroying the bucket
  force_destroy = false
}