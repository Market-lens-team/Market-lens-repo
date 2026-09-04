# Display the name of the GCS bucket after Terraform creates it
output "raw_bucket_name" {

  # Explain what this output represents
  description = "Name of the MarketLens raw-data GCS bucket."

  # Get the actual name of the bucket
  value = google_storage_bucket.raw_data.name
}


# Display the URL of the GCS bucket
output "raw_bucket_url" {

  # Explain what this output represents
  description = "URL of the MarketLens raw-data GCS bucket."

  # Get the URL of the bucket
  value = google_storage_bucket.raw_data.url
}