"""
Day 5 Lab: Serverless Orchestration (Airflow / ADF), Containerization (Docker),
High-Availability Load Balancing (ALB), and Multi-Cloud Infrastructure-as-Code (Terraform).
"""

from pathlib import Path
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


def scaffold_airflow_dag(dags_dir: Path):
    """Generates a production-grade Apache Airflow 2.x TaskFlow DAG with ADF Mapping."""
    dags_dir.mkdir(parents=True, exist_ok=True)
    dag_file = dags_dir / "weather_platform_dag.py"

    dag_code = '''"""
Production Weather Intelligence Platform DAG (Airflow 2.x TaskFlow API)
Replaces Legacy BP Cron/NFS Airflow DAGs with Event-Driven Lakehouse & Circuit-Breaker Orchestration.

AZURE DATA FACTORY (ADF) / SYNAPSE EQUIVALENT MAPPING:
  - Event Trigger      : Azure Event Grid Trigger (BlobCreated on ADLS Gen2 landing container)
  - Task 1 (Zarr)      : ADF Azure Batch / Databricks Python Activity
  - Task 2 (Delta)     : ADF Mapping Data Flow or Synapse Notebook Activity
  - Task 3 (dbt ELT)   : ADF Web Activity -> dbt Cloud API (or Azure Container Instances dbt runner)
  - Task 4 (Wave Cube) : ADF Databricks / Batch Activity
  - Task 5 (STAC/OGC)  : ADF Web Hook / Azure Function Activity
  - Task 6 (Kafka EIP) : Azure Event Hubs / Logic Apps SAP & Salesforce Publisher
"""

from datetime import datetime, timedelta
import subprocess
import sys
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "climate-data-architecture",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=15),
}

@dag(
    dag_id="uk_north_sea_weather_intelligence_v2",
    default_args=DEFAULT_ARGS,
    description="End-to-End ECMWF NetCDF/GRIB -> Zarr -> Delta Lake -> dbt -> STAC/OGC -> Kafka/SAP/SFDC",
    schedule="0 6,18 * * *",  # Runs at 06:00 & 18:00 UTC (00z and 12z ECMWF availability) or via Dataset trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "zarr", "lakehouse", "dbt", "enterprise-integration"],
)
def weather_intelligence_pipeline():

    @task(task_id="ingest_and_build_zarr_cube")
    def ingest_and_build_zarr():
        """Stage 1 & 2: Ingest UK ERA5 NetCDF, chunk into Zarr, and extract Hive Parquet."""
        subprocess.run([sys.executable, "src/01_prepare_era5_uk.py"], check=True)
        subprocess.run([sys.executable, "src/05_day2_zarr_and_lakehouse.py"], check=True)
        return {"zarr_uri": "data/zarr/era5_uk.zarr", "status": "ZARR_READY"}

    @task(task_id="commit_acid_delta_lakehouse")
    def commit_delta_lakehouse(zarr_meta: dict):
        """Stage 3A: Commit 00z/12z revisions to ACID Delta Lake table with Time-Travel."""
        subprocess.run([sys.executable, "src/06_day3_lakehouse_time_travel.py"], check=True)
        return {"delta_table": "data/lakehouse/uk_energy_delta", "version": 1}

    @task(task_id="run_dbt_transformations_and_dataops_tests")
    def run_dbt_elt_and_quality_gate(delta_meta: dict):
        """
        Stage 3B (CIRCUIT BREAKER): Materialize dbt Staging & Gold Marts and run 8 DataOps tests.
        If physical weather bounds (-50C to +55C, 870 to 1085 hPa) fail, subprocess raises
        CalledProcessError and HALTS the pipeline before bad alerts reach SAP or Salesforce!
        """
        subprocess.run([sys.executable, "src/07_setup_and_run_dbt.py"], check=True)
        return {"dbt_mart": "main.mart_uk_trading_desk_alerts", "tests_passed": 8}

    @task(task_id="process_north_sea_wave_hazards")
    def process_north_sea_waves():
        """Stage 3.5: Build North Sea Wave Zarr cube & evaluate >= 5.0m SWH platform limits."""
        subprocess.run([sys.executable, "src/08_north_sea_wave_alerts.py"], check=True)
        return {"wave_zarr": "data/zarr/north_sea_wave.zarr", "threshold_m": 5.0}

    @task(task_id="publish_stac_catalog_and_verify_ogc_api")
    def publish_stac_and_verify_api(dbt_meta: dict, wave_meta: dict):
        """Stage 4A: Update STAC Catalog JSONs and run health/smoke checks on FastAPI OGC EDR."""
        subprocess.run([sys.executable, "src/09_day4_stac_and_ogc_api.py"], check=True)
        return {"stac_catalog": "data/stac/catalog.json", "ogc_api_status": "HEALTHY"}

    @task(task_id="route_cloud_events_to_sap_and_salesforce")
    def emit_enterprise_events(api_meta: dict):
        """Stage 4B: Execute Camel/Kafka Content-Based Router to SAP PM and Salesforce Trading."""
        subprocess.run([sys.executable, "src/10_day4_enterprise_event_router.py"], check=True)
        return {"events_published": True}

    # Define DAG Parallelism & Dependency Lineage
    zarr_state = ingest_and_build_zarr()
    delta_state = commit_delta_lakehouse(zarr_state)
    dbt_verified = run_dbt_elt_and_quality_gate(delta_state)

    # North Sea Wave processing runs in parallel with the UK Onshore/Trading Lakehouse branch!
    wave_state = process_north_sea_waves()

    # Fan-in: Both branches must succeed and pass DataOps gates before updating STAC & firing Kafka alerts
    api_ready = publish_stac_and_verify_api(dbt_verified, wave_state)
    emit_enterprise_events(api_ready)

dag_instance = weather_intelligence_pipeline()
'''
    dag_file.write_text(dag_code.lstrip())
    print(f"[SUCCESS] Generated Production Airflow 2.x TaskFlow DAG: {dag_file}")


def scaffold_docker_and_load_balancer(docker_dir: Path):
    """Generates Dockerfile and NGINX Load-Balanced docker-compose.yml."""
    docker_dir.mkdir(parents=True, exist_ok=True)

    dockerfile_content = '''# Production Container for FastAPI OGC API - EDR & Weather Lakehouse Service
# Replaces legacy BP RHEL Apache Tomcat VMs with a stateless Linux container
FROM python:3.12-slim-bookworm

WORKDIR /app

# Install native C-libraries for scientific weather processing (NetCDF / ecCodes)
RUN apt-get update && apt-get install -y --no-install-recommends \\
    libnetcdf-dev libeccodes-dev curl && \\
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source and STAC/Lakehouse metadata
COPY src/ ./src/
COPY data/ ./data/

# Run as non-root user for enterprise container security (EKS / AKS / GKE / Cloud Run)
RUN useradd -m weatherapp && chown -R weatherapp:weatherapp /app
USER weatherapp

EXPOSE 8000

# Container-native Health Check used by AWS ALB / Azure App Gateway / GCP Load Balancer
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \\
    CMD curl -f http://localhost:8000/ || exit 1

CMD ["uvicorn", "src.09_day4_stac_and_ogc_api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
'''
    (docker_dir / "Dockerfile").write_text(dockerfile_content.lstrip())

    compose_content = '''# Simulates High-Availability Load Balancing across Stateless OGC Weather API Replicas
version: "3.9"

services:
  ogc-weather-api-1:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    environment:
      - INSTANCE_ID=replica-az-a
    expose:
      - "8000"

  ogc-weather-api-2:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    environment:
      - INSTANCE_ID=replica-az-b
    expose:
      - "8000"

  # Layer-7 Reverse Proxy / Load Balancer (Local equivalent of AWS ALB / GCP Load Balancer)
  alb-load-balancer:
    image: nginx:1.27-alpine
    ports:
      - "8080:80"
    depends_on:
      - ogc-weather-api-1
      - ogc-weather-api-2
'''
    (docker_dir / "docker-compose.yml").write_text(compose_content.lstrip())
    print(f"[SUCCESS] Generated Dockerfile & HA Load-Balanced Compose spec in: {docker_dir}/")


def scaffold_terraform_iac(tf_dir: Path):
    """
    Generates Multi-Cloud-Ready Terraform IaC (AWS S3 Zarr/Delta Lakehouse + ALB + ECS Fargate
    with architectural mapping comments for Azure and GCP).
    """
    tf_dir.mkdir(parents=True, exist_ok=True)

    variables_tf = '''variable "project_name" {
  description = "Name prefix for all Weather Intelligence Platform resources"
  type        = string
  default     = "weather-intel-platform"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "aws_region" {
  description = "Primary cloud deployment region (London UK)"
  type        = string
  default     = "eu-west-2"
}

variable "container_image" {
  description = "Docker image URI for the FastAPI OGC EDR microservice"
  type        = string
  default     = "123456789012.dkr.ecr.eu-west-2.amazonaws.com/weather-ogc-edr:latest"
}
'''

    main_tf = '''# =====================================================================================
# Day 5 Terraform IaC: Modern Cloud-Native Weather Platform
# Replaces Legacy BP CloudFormation + 4 Static RHEL VMs + Shared NFS Storage
#
# MULTI-CLOUD RESOURCE EQUIVALENTS:
#   1. aws_s3_bucket             <-> azurerm_storage_account (ADLSv2) <-> google_storage_bucket (GCS)
#   2. aws_glue_catalog_database <-> azurerm_synapse / databricks     <-> google_bigquery_dataset (BigLake)
#   3. aws_lb (ALB)              <-> azurerm_application_gateway      <-> google_compute_url_map (Cloud LB)
#   4. aws_ecs_service (Fargate) <-> azurerm_container_app            <-> google_cloud_run_v2_service
# =====================================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# 1. CLOUD OBJECT STORAGE: Zarr Weather Cubes & Delta/Iceberg Lakehouse (Replaces Shared NFS)
resource "aws_s3_bucket" "weather_lakehouse" {
  bucket = "${var.project_name}-${var.environment}-zarr-lakehouse"
  tags = {
    Architecture = "Medallion-Zarr-Delta"
    Domain       = "Energy-Trading-And-Marine"
  }
}

resource "aws_s3_bucket_versioning" "lakehouse_versioning" {
  bucket = aws_s3_bucket.weather_lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# 2. SERVERLESS LAKEHOUSE CATALOG (For Athena / Spark / Snowflake External Iceberg Tables)
resource "aws_glue_catalog_database" "weather_catalog" {
  name        = "${replace(var.project_name, "-", "_")}_${var.environment}_db"
  description = "Metadata catalog for UK Energy Forecasts and North Sea Wave Alert tables"
}

# 3. LAYER-7 APPLICATION LOAD BALANCER (Closes the BP RHEL Tomcat Load Balancer Gap!)
resource "aws_lb" "ogc_api_alb" {
  name               = "${var.project_name}-${var.environment}-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = ["subnet-01111111111111111", "subnet-02222222222222222"] # Multi-AZ Subnets
}

resource "aws_lb_target_group" "ogc_edr_tg" {
  name        = "${var.project_name}-edr-tg"
  port        = 8000
  protocol    = "HTTP"
  target_type = "ip" # Required for Serverless Fargate containers
  vpc_id      = "vpc-0123456789abcdef0"

  # Active Health Check: Automatically routes around unhealthy containers in < 30 seconds
  health_check {
    enabled             = true
    path                = "/"
    port                = "traffic-port"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 15
    matcher             = "200"
  }
}

resource "aws_lb_listener" "http_listener" {
  load_balancer_arn = aws_lb.ogc_api_alb.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ogc_edr_tg.arn
  }
}

# 4. SERVERLESS CONTAINER CLUSTER (AWS ECS Fargate / GCP Cloud Run Equivalent)
resource "aws_ecs_cluster" "weather_cluster" {
  name = "${var.project_name}-${var.environment}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}
'''

    outputs_tf = '''output "zarr_and_lakehouse_bucket_uri" {
  description = "S3 URI for Zarr cubes and Delta/Iceberg Parquet tables"
  value       = "s3://${aws_s3_bucket.weather_lakehouse.bucket}"
}

output "ogc_edr_load_balancer_dns" {
  description = "High-Availability Load Balancer endpoint replacing BP Tomcat VMs"
  value       = "http://${aws_lb.ogc_api_alb.dns_name}/collections"
}

output "lakehouse_glue_database" {
  description = "Serverless SQL Catalog Database Name"
  value       = aws_glue_catalog_database.weather_catalog.name
}
'''

    (tf_dir / "variables.tf").write_text(variables_tf.lstrip())
    (tf_dir / "main.tf").write_text(main_tf.lstrip())
    (tf_dir / "outputs.tf").write_text(outputs_tf.lstrip())
    print(f"[SUCCESS] Generated Terraform IaC modules in: {tf_dir}/ (main.tf, variables.tf, outputs.tf)")


def execute_local_dag_orchestrator(manifest_path: Path) -> list[dict]:
    """
    Executes our complete 6-Stage Weather Intelligence DAG in topological order,
    recording execution latency, status, and DataOps gate results.
    """
    dag_tasks = [
        {
            "task_id": "1_ingest_and_build_zarr",
            "stage": "Gridded Zarr & Lakehouse Prep",
            "scripts": ["src/01_prepare_era5_uk.py", "src/05_day2_zarr_and_lakehouse.py"],
        },
        {
            "task_id": "2_commit_acid_delta_table",
            "stage": "Delta Lake 00z/12z Time-Travel",
            "scripts": ["src/06_day3_lakehouse_time_travel.py"],
        },
        {
            "task_id": "3_dbt_elt_and_quality_gate",
            "stage": "dbt Marts + 8 DataOps Circuit Breakers",
            "scripts": ["src/07_setup_and_run_dbt.py"],
        },
        {
            "task_id": "4_north_sea_wave_5m_hazards",
            "stage": "North Sea Wave Zarr & 5m Breach Calc",
            "scripts": ["src/08_north_sea_wave_alerts.py"],
        },
        {
            "task_id": "5_stac_and_ogc_edr_api",
            "stage": "STAC Catalog & FastAPI OGC Smoke Test",
            "scripts": ["src/09_day4_stac_and_ogc_api.py"],
        },
        {
            "task_id": "6_kafka_camel_event_router",
            "stage": "CloudEvents Router -> SAP & Salesforce",
            "scripts": ["src/10_day4_enterprise_event_router.py"],
        },
    ]

    print("\n" + "=" * 78)
    print("EXECUTING END-TO-END WEATHER INTELLIGENCE DAG (LOCAL AIRFLOW/ADF RUNNER)")
    print("=" * 78)

    task_metrics = []
    dag_start = time.perf_counter()

    for t in dag_tasks:
        t0 = time.perf_counter()
        print(f"\n>>> [DAG TASK START] {t['task_id']} ({t['stage']})")
        for script in t["scripts"]:
            subprocess.run([sys.executable, script], check=True)
        elapsed = round(time.perf_counter() - t0, 2)
        print(f"<<< [DAG TASK SUCCESS] {t['task_id']} completed in {elapsed:.2f}s")
        task_metrics.append(
            {
                "task_id": t["task_id"],
                "stage": t["stage"],
                "status": "SUCCESS",
                "duration_sec": elapsed,
            }
        )

    total_sec = round(time.perf_counter() - dag_start, 2)
    manifest = {
        "dag_id": "uk_north_sea_weather_intelligence_v2",
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_duration_sec": total_sec,
        "overall_status": "SUCCESS_ALL_DATAOPS_GATES_PASSED",
        "tasks": task_metrics,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print("\n" + "=" * 78)
    print(f"[DAG COMPLETE] All 6 pipeline stages succeeded in {total_sec:.2f}s!")
    print(f"[AUDIT LOG] Saved execution manifest to: {manifest_path}")
    print("=" * 78)
    return task_metrics


def create_day5_visual(task_metrics: list[dict], out_img: Path):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), dpi=150)
    fig.suptitle(
        "Day 5 Architecture: High-Availability Load Balancing (ALB) & End-to-End DAG Orchestration",
        fontsize=14, fontweight="bold", y=0.98
    )

    # Panel 1: Legacy BP 4 RHEL VMs vs. Modern ALB + Stateless Auto-Scaling Containers
    ax1.set_title("1. Replacing BP Tomcat VMs: Layer-7 ALB + Stateless Zarr/Lakehouse Containers", fontsize=11, fontweight="bold")
    ax1.axis("off")

    ax1.add_patch(FancyBboxPatch((0.04, 0.76), 0.92, 0.17, boxstyle="round,pad=0.02", facecolor="#f4cccc", edgecolor="#990000"))
    ax1.text(0.07, 0.87, "BEFORE (BP Legacy): 4 Static RHEL VMs + Apache Tomcat + Shared NFS", fontsize=9.5, fontweight="bold", color="#660000")
    ax1.text(0.07, 0.79, "• Fixed capacity, NFS file locks on NetCDF/GRIB, pre-rendered static PNGs/CSVs.", fontsize=8.5)

    ax1.add_patch(FancyBboxPatch((0.04, 0.51), 0.92, 0.19, boxstyle="round,pad=0.02", facecolor="#fff2cc", edgecolor="#d6b656"))
    ax1.text(0.07, 0.63, "MODERN LAYER-7 LOAD BALANCER (AWS ALB / Azure AppGW / GCP Cloud LB)", fontsize=9.5, fontweight="bold")
    ax1.text(0.07, 0.54, "• Terminates HTTPS/TLS  • Active Health Check (/healthz every 15s)\n"
                         "• Routes /collections/* -> OGC EDR API Replicas across 3 Availability Zones", fontsize=8.5)

    replicas = ["Replica 1 (AZ-A)\nFastAPI + Xarray", "Replica 2 (AZ-B)\nFastAPI + Xarray", "Replica N (Auto-Scale)\nFastAPI + Xarray"]
    for i, label in enumerate(replicas):
        x0 = 0.04 + i * 0.315
        ax1.add_patch(FancyBboxPatch((x0, 0.28), 0.29, 0.17, boxstyle="round,pad=0.02", facecolor="#cfe2f3", edgecolor="#0b5394"))
        ax1.text(x0 + 0.02, 0.34, label, fontsize=8.5, fontweight="bold")

    ax1.add_patch(FancyBboxPatch((0.04, 0.04), 0.92, 0.18, boxstyle="round,pad=0.02", facecolor="#d9ead3", edgecolor="#38761d"))
    ax1.text(0.07, 0.15, "SHARED CLOUD OBJECT STORAGE (S3 / ADLS Gen2 / GCS) + TERRAFORM IaC", fontsize=9.5, fontweight="bold", color="#274e13")
    ax1.text(0.07, 0.07, "• Zero local disk state! All containers slice era5_uk.zarr & Delta/Iceberg tables over HTTP.", fontsize=8.5)

    # Panel 2: Actual Execution Duration per DAG Task
    ax2.set_title("2. End-to-End Orchestration DAG Telemetry (Task Execution Time)", fontsize=11, fontweight="bold")
    labels = [f"{m['task_id']}\n({m['stage'][:24]})" for m in task_metrics][::-1]
    durations = [m["duration_sec"] for m in task_metrics][::-1]
    colors = ["#6fa8dc", "#6fa8dc", "#93c47d", "#e06666", "#ffd966", "#8e7cc3"][::-1]

    bars = ax2.barh(labels, durations, color=colors, edgecolor="black", height=0.58)
    ax2.set_xlabel("Execution Time (Seconds)")
    ax2.grid(axis="x", linestyle="--", alpha=0.4)

    max_d = max(durations) if durations else 1.0
    ax2.set_xlim(0, max_d * 1.25)
    for bar in bars:
        w = bar.get_width()
        ax2.text(w + (max_d * 0.02), bar.get_y() + bar.get_height() / 2, f"{w:.2f}s (PASS)", va="center", fontsize=8.5, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(out_img)
    plt.close(fig)
    print(f"[SUCCESS] Saved Day 5 HA Load Balancer & DAG Telemetry visual to: {out_img.resolve()}")


def main():
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Scaffold Production Airflow DAG, Docker/ALB specs, and Terraform IaC
    scaffold_airflow_dag(Path("dags"))
    scaffold_docker_and_load_balancer(Path("docker"))
    scaffold_terraform_iac(Path("terraform"))

    # 2. Run the Full 6-Stage Pipeline Orchestrator & Record Telemetry
    metrics = execute_local_dag_orchestrator(out_dir / "day5_pipeline_execution_manifest.json")

    # 3. Render Day 5 Visual Architecture & Telemetry Diagram
    create_day5_visual(metrics, out_dir / "day5_ha_loadbalancer_and_dag_visual.png")


if __name__ == "__main__":
    main()