terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  credentials = file("${path.module}/credentials.json")
  project     = var.project_id
  region      = var.region
  zone        = var.zone
}

# ==========================================
# 1. GOOGLE CLOUD API ENABLEMENT
# ==========================================
locals {
  services_to_enable = [
    "storage.googleapis.com",         # Cloud Storage
    "bigquery.googleapis.com",        # BigQuery
    "artifactregistry.googleapis.com" # Artifact Registry (Docker images)
  ]
}

resource "google_project_service" "enabled_apis" {
  for_each           = toset(local.services_to_enable)
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# ==========================================
# 2. GOOGLE CLOUD STORAGE (Data Lake)
# ==========================================
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30 # Delete files older than 30 days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.enabled_apis]
}

# ==========================================
# 3. GOOGLE BIGQUERY (Data Warehouse)
# ==========================================
resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id                 = var.bq_dataset_id
  location                   = var.region
  description                = "Dataset containing processed ENTSO-E electricity data"
  delete_contents_on_destroy = true

  depends_on = [google_project_service.enabled_apis]
}

# ==========================================
# 4. GOOGLE ARTIFACT REGISTRY (Docker Images)
# ==========================================
resource "google_artifact_registry_repository" "docker_repo" {
  location      = var.region
  repository_id = "entsoe"
  description   = "Docker repository for ENTSO-E data engineering images"
  format        = "DOCKER"

  depends_on = [google_project_service.enabled_apis]
}
