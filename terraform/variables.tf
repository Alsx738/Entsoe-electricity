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
