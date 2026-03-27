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

locals {
  dataproc_staging_bucket_name = var.dataproc_staging_bucket_name != "" ? var.dataproc_staging_bucket_name : var.bucket_name
}

# ==========================================
# 6. DATAPROC CLUSTER
# NOTE: After creation, the Dataproc cluster is running (ON) and incurs costs.
# It is recommended to stop it manually when not in use.
# ==========================================
resource "google_dataproc_cluster" "entsoe_cluster" {
  name    = var.dataproc_cluster_name
  region  = var.region
  project = var.project_id

  cluster_config {
    staging_bucket = local.dataproc_staging_bucket_name

    master_config {
      num_instances = 1
      machine_type  = var.dataproc_master_machine_type

      disk_config {
        boot_disk_type    = var.dataproc_master_boot_disk_type
        boot_disk_size_gb = var.dataproc_master_boot_disk_size_gb
      }
    }

    worker_config {
      num_instances = var.dataproc_worker_num_instances
      machine_type  = var.dataproc_worker_machine_type

      disk_config {
        boot_disk_type    = var.dataproc_worker_boot_disk_type
        boot_disk_size_gb = var.dataproc_worker_boot_disk_size_gb
      }
    }

    software_config {
      image_version = var.dataproc_image_version

      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
        "spark:spark.driver.memory"            = "10g"
        "spark:spark.executor.memory"          = "2g"
        "spark:spark.sql.shuffle.partitions"   = "12"
      }
    }

    gce_cluster_config {
      zone             = var.dataproc_zone
      network          = var.dataproc_network_name
      internal_ip_only = var.dataproc_internal_ip_only

      service_account_scopes = var.dataproc_service_account_scopes

      shielded_instance_config {
        enable_secure_boot          = var.dataproc_enable_secure_boot
        enable_vtpm                 = var.dataproc_enable_vtpm
        enable_integrity_monitoring = var.dataproc_enable_integrity_monitoring
      }
    }
  }

  lifecycle {
    ignore_changes = [cluster_config[0].software_config[0].override_properties]
  }

  depends_on = [google_project_service.enabled_apis]
}

# ==========================================
# 7. KESTRA VM + PUBLIC ACCESS FIREWALL
# ==========================================
resource "google_compute_resource_policy" "kestra_vm_schedule" {
  name   = var.kestra_vm_schedule_policy_name
  region = var.region

  instance_schedule_policy {
    vm_start_schedule {
      schedule = var.kestra_vm_start_schedule
    }

    vm_stop_schedule {
      schedule = var.kestra_vm_stop_schedule
    }

    time_zone = var.kestra_vm_schedule_time_zone
  }

  depends_on = [google_project_service.enabled_apis]
}

resource "google_compute_instance" "kestra_vm" {
  name         = var.kestra_vm_name
  machine_type = var.kestra_vm_machine_type
  zone         = var.kestra_vm_zone
  tags         = var.kestra_vm_tags

  # This script runs as root when the VM starts
  metadata_startup_script = <<-EOT
    #!/bin/bash
    timedatectl set-timezone Europe/Rome
  EOT

  boot_disk {
    initialize_params {
      image = var.kestra_vm_boot_image
      size  = var.kestra_vm_boot_disk_size_gb
    }
  }

  network_interface {
    network = var.kestra_vm_network_name

    dynamic "access_config" {
      for_each = var.kestra_vm_enable_public_ip ? [1] : []
      content {}
    }
  }

  resource_policies = concat(
    [google_compute_resource_policy.kestra_vm_schedule.id],
    var.kestra_vm_resource_policies
  )

  depends_on = [google_project_service.enabled_apis]
}

resource "google_compute_firewall" "allow_kestra_ui" {
  name    = var.kestra_vm_firewall_name
  network = var.kestra_vm_network_name

  allow {
    protocol = "tcp"
    ports    = var.kestra_vm_allowed_tcp_ports
  }

  source_ranges = var.kestra_vm_allowed_source_ranges
  target_tags   = var.kestra_vm_tags

  depends_on = [google_project_service.enabled_apis]
}