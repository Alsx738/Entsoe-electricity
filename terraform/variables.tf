variable "project_id" {
  description = "The Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "The region in which to deploy resources (e.g. europe-west1)"
  type        = string
  default     = "europe-west4"
}

variable "zone" {
  description = "The zone for zonal resources"
  type        = string
  default     = "europe-west4-a"
}

variable "bucket_name" {
  description = "The GCS bucket name for the data lake"
  type        = string
}

variable "bq_raw_dataset_id" {
  description = "The BigQuery dataset ID for the raw data warehouse (external tables)"
  type        = string
  default     = "entsoe_warehouse"
}

variable "bq_analytics_dataset_id" {
  description = "The BigQuery dataset ID for final analytics (dbt marts)"
  type        = string
  default     = "entsoe_analytics_dev"
}

variable "dataproc_cluster_name" {
  description = "Dataproc cluster name"
  type        = string
  default     = "cluster-cdca"
}

variable "dataproc_staging_bucket_name" {
  description = "Optional staging bucket for Dataproc; if empty, bucket_name is used"
  type        = string
  default     = ""
}

variable "dataproc_zone" {
  description = "Zone used by Dataproc cluster VMs"
  type        = string
  default     = "europe-west4-c"
}

variable "dataproc_network_name" {
  description = "VPC network name used by Dataproc cluster"
  type        = string
  default     = "default"
}

variable "dataproc_master_machine_type" {
  description = "Master node machine type"
  type        = string
  default     = "n4-highmem-2"
}

variable "dataproc_master_boot_disk_type" {
  description = "Master node boot disk type"
  type        = string
  default     = "hyperdisk-balanced"
}

variable "dataproc_master_boot_disk_size_gb" {
  description = "Master node boot disk size in GB"
  type        = number
  default     = 100
}

variable "dataproc_worker_num_instances" {
  description = "Number of Dataproc worker nodes"
  type        = number
  default     = 3
}

variable "dataproc_worker_machine_type" {
  description = "Worker node machine type"
  type        = string
  default     = "n4-highmem-2"
}

variable "dataproc_worker_boot_disk_type" {
  description = "Worker node boot disk type"
  type        = string
  default     = "hyperdisk-balanced"
}

variable "dataproc_worker_boot_disk_size_gb" {
  description = "Worker node boot disk size in GB"
  type        = number
  default     = 100
}

variable "dataproc_image_version" {
  description = "Dataproc image version"
  type        = string
  default     = "2.2-debian12"
}

variable "dataproc_internal_ip_only" {
  description = "If true, VMs are created with internal IP only"
  type        = bool
  default     = false
}

variable "dataproc_service_account_scopes" {
  description = "Service account scopes for Dataproc VMs"
  type        = list(string)
  default     = ["https://www.googleapis.com/auth/cloud-platform"]
}

variable "dataproc_enable_secure_boot" {
  description = "Shielded VM secure boot"
  type        = bool
  default     = false
}

variable "dataproc_enable_vtpm" {
  description = "Shielded VM vTPM"
  type        = bool
  default     = false
}

variable "dataproc_enable_integrity_monitoring" {
  description = "Shielded VM integrity monitoring"
  type        = bool
  default     = false
}

variable "kestra_vm_name" {
  description = "Compute Engine VM name used to host Kestra"
  type        = string
  default     = "kestra-vm"
}

variable "kestra_vm_machine_type" {
  description = "Compute Engine machine type for Kestra VM"
  type        = string
  default     = "n2-standard-2"
}

variable "kestra_vm_zone" {
  description = "Zone where Kestra VM is created"
  type        = string
  default     = "europe-west4-c"
}

variable "kestra_vm_boot_image" {
  description = "Boot image for Kestra VM"
  type        = string
  default     = "projects/debian-cloud/global/images/debian-12-bookworm-v20260310"
}

variable "kestra_vm_boot_disk_size_gb" {
  description = "Boot disk size in GB for Kestra VM"
  type        = number
  default     = 40
}

variable "kestra_vm_network_name" {
  description = "VPC network used by Kestra VM"
  type        = string
  default     = "default"
}

variable "kestra_vm_enable_public_ip" {
  description = "If true, attach an ephemeral public IP to Kestra VM"
  type        = bool
  default     = true
}

variable "kestra_vm_schedule_policy_name" {
  description = "Name of the VM schedule resource policy"
  type        = string
  default     = "kestra-wake-up"
}

variable "kestra_vm_start_schedule" {
  description = "Cron schedule for VM start"
  type        = string
  default     = "50 2 * * *"
}

variable "kestra_vm_stop_schedule" {
  description = "Cron schedule for VM stop"
  type        = string
  default     = "0 4 * * *"
}

variable "kestra_vm_schedule_time_zone" {
  description = "Time zone used by VM start/stop schedule"
  type        = string
  default     = "Europe/Rome"
}

variable "kestra_vm_resource_policies" {
  description = "Optional additional resource policies to attach to Kestra VM"
  type        = list(string)
  default     = []
}

variable "kestra_vm_tags" {
  description = "Network tags applied to Kestra VM and used by firewall rules"
  type        = list(string)
  default     = ["kestra-ui"]
}

variable "kestra_vm_firewall_name" {
  description = "Firewall rule name for Kestra VM public access"
  type        = string
  default     = "allow-kestra-ui"
}

variable "kestra_vm_allowed_tcp_ports" {
  description = "Public TCP ports to expose for Kestra VM"
  type        = list(string)
  default     = ["80", "443", "8080"]
}

variable "kestra_vm_allowed_source_ranges" {
  description = "Source CIDR ranges allowed to reach Kestra VM exposed ports"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
