resource "aws_glue_catalog_database" "nyc_taxi" {
  name        = "nyc_taxi_dev"
  description = "AWS Glue Data Catalog database for the NYC taxi data lake."
}

resource "aws_athena_workgroup" "analytics" {
  name          = "nyc-taxi-analytics-dev"
  description   = "Controlled Athena workgroup for NYC taxi analytics."
  state         = "ENABLED"
  force_destroy = false

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    requester_pays_enabled             = false
    bytes_scanned_cutoff_per_query     = 1073741824

    engine_version {
      selected_engine_version = "AUTO"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

output "glue_catalog_database_name" {
  description = "Name of the Glue Data Catalog database."
  value       = aws_glue_catalog_database.nyc_taxi.name
}

output "athena_workgroup_name" {
  description = "Name of the controlled Athena workgroup."
  value       = aws_athena_workgroup.analytics.name
}