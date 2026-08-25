# Terraform configuration block
terraform {

  # Minimum Terraform version required for this project
  required_version = ">= 1.6.0"

  # Define the providers required by this project
  required_providers {

    # Google Cloud provider
    google = {

      # Official provider source from Terraform Registry
      source = "hashicorp/google"

      # Use the Google provider 7.x series
      version = "~> 7.0"
    }
  }
}