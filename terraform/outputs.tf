output "zarr_and_lakehouse_bucket_uri" {
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
