# Terraform Infrastructure for ENTSO-E Pipeline

This folder provisions the core Google Cloud infrastructure for the ENTSO-E data platform.

The goal is to support:
- Daily ingestion and Spark processing.
- Daily incremental dbt transformations.
- Automated orchestration through Kestra.

## What Terraform Creates

Terraform creates and manages:
- Required Google APIs.
- Data Lake bucket in Cloud Storage.
- BigQuery datasets (raw warehouse and analytics mart).
- Artifact Registry repository for Docker images.
- Network resources for Spark workloads.
- A Dataproc cluster (non-serverless).
- A dedicated VM for Kestra, with public access firewall and schedule policy.

## Main Design Choice: Stay Under 12 vCPUs

The infrastructure sizing is intentionally conservative to remain under a 12 vCPU budget while still running Dataproc and Kestra together.

Current sizing:
- Dataproc master: 1 x n4-highmem-2 (2 vCPUs).
- Dataproc workers: 2 x n4-highmem-2 (4 vCPUs total).
- Kestra VM: 1 x n2-standard-2 (2 vCPUs).

Total: 8 vCPUs.

This leaves margin under 12 vCPUs and allows stable operation of:
- Daily ingestion orchestration.
- Incremental dbt runs.
- Dataproc Spark transformations.

## Dataproc Configuration

The Dataproc cluster is configured as a standard (non-serverless) cluster with:
- Dedicated master and worker nodes.
- Hyperdisk boot disks.
- Spark runtime properties tuned for this workload.
- Public IP for vm


Important behavior:
- Right after Terraform creates the cluster, it is running.
- A running Dataproc cluster generates cost.
- If you are not using it, stop it.

Operational model used in this project:
- Kestra is responsible for Dataproc lifecycle during pipeline execution (start/stop around jobs), so compute is used only when needed.

## Kestra VM Configuration

The VM is intended to host Kestra and keep the UI reachable.

Current characteristics:
- Machine type: n2-standard-2.
- Boot image: Debian 12.
- Boot disk size: configurable, currently 40 GB.
- Public IP enabled.
- Firewall rule exposing TCP ports 80, 443, and 8080.
- Source ranges configurable (currently open to 0.0.0.0/0 in variable example).

Important behavior:
- Right after Terraform creates the VM, it is running.
- A running VM generates cost.
- If you are not using it, stop it.

VM schedule policy:
- A Compute Engine resource policy is created and attached to the VM.
- Start schedule: 02:50 (Europe/Rome).
- Stop schedule: 04:00 (Europe/Rome).

This supports automatic availability before the daily Kestra jobs.

## Cost and Safety Notes

- Dataproc and VM both start in running state after creation.
- If you are not executing pipelines, stop resources to avoid unnecessary charges.
- Restrict VM firewall source ranges when possible instead of using 0.0.0.0/0.
- Keep Kestra credentials and secrets secure.

## Configuration via terraform.tfvars

All major parameters are configurable through variables and tfvars, including:
- Project, region, zone.
- Dataproc cluster sizing and disk settings.
- Dataproc networking and runtime behavior.
- Kestra VM machine type, disk size, network, public IP.
- Firewall ports and allowed source ranges.
- VM start/stop schedules and timezone.

Use terraform.tfvars for active values and terraform.tfvars.example as template.

## Typical Workflow

1. Update terraform.tfvars with your environment values.
2. Run Terraform plan/apply.
3. Confirm Dataproc and VM status.
4. If resources are idle, stop them to reduce cost.
5. Let Kestra orchestrate daily ingestion and incremental dbt pipeline execution.
6. Check the name that you give to cluster: should be the same used in kesta .yml; they are hardcoded(lazy to pass throught env)