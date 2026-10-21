variable "project_name" {
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
