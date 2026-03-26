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
    "cloudresourcemanager.googleapis.com", # Required for project-level service discovery/management
    "serviceusage.googleapis.com",         # Required to enable/disable APIs programmatically
    "storage.googleapis.com",         # Cloud Storage
    "bigquery.googleapis.com",        # BigQuery
    "artifactregistry.googleapis.com", # Artifact Registry (Docker images)
    "dataproc.googleapis.com", # <--- OBBLIGATORIA
    "compute.googleapis.com",   # <--- NECESSARIA PER LA RETE
    "iam.googleapis.com" # <--- Aggiunta per la gestione dei permessi
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
# 3. GOOGLE BIGQUERY (Data Warehouse & Analytics)
# ==========================================
resource "google_bigquery_dataset" "raw_warehouse" {
  dataset_id                 = var.bq_raw_dataset_id
  location                   = var.region
  description                = "Dataset containing raw ENTSO-E external tables from Data Lake"
  delete_contents_on_destroy = true

  depends_on = [google_project_service.enabled_apis]
}

resource "google_bigquery_dataset" "analytics_mart" {
  dataset_id                 = var.bq_analytics_dataset_id
  location                   = var.region
  description                = "Dataset containing final modeled ENTSO-E facts and dimensions from dbt"
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

# ==========================================
# 5. NETWORKING PER DATAPROC SERVERLESS
# ==========================================

# Dedicated VPC for Dataproc workloads.
resource "google_compute_network" "vpc" {
  name                    = "entsoe-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.enabled_apis]
}

# Subnet with Private Google Access enabled.
resource "google_compute_subnetwork" "subnet" {
  name                     = "entsoe-spark-subnet"
  ip_cidr_range            = "10.0.0.0/24"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
}

# Allow internal communication within the subnet.
resource "google_compute_firewall" "allow_internal_spark" {
  name    = "allow-internal-spark"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = ["10.0.0.0/24"]
}