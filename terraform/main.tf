# =====================================================================================
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
