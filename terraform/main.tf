

terraform {
  required_version = ">= 0.14"

  required_providers {
    # Cloud Run support was added on 3.3.0
    google = {
      source  = "hashicorp/google"
      version = ">= 3.5"
    }
  }
}

locals {
  project_id = jsondecode(file(var.credentials)).project_id
}

provider "google" {
  credentials = file(var.credentials)
  project     = local.project_id
  region      = var.region
}