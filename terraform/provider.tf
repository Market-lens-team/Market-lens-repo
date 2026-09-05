# Configure Google Cloud as the provider for Terraform
provider "google" {

  # Specify the GCP project where resources will be created
  project = var.project_id

  # Specify the default GCP region for the resources
  region = var.region
}