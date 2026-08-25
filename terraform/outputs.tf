output "raw_bucket_name" {
  description = "Name of the MarketLens raw-data GCS bucket."
  value       = google_storage_bucket.raw_data.name
}

output "raw_bucket_url" {
  description = "URL of the MarketLens raw-data GCS bucket."
  value       = google_storage_bucket.raw_data.url
}