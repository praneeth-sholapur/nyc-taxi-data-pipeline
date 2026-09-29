locals {
  glue_transform_job_name = "nyc-taxi-transform-dev"
  glue_transform_script   = "glue-scripts/transform_taxi.py"
}

resource "aws_s3_object" "glue_transform_script" {
  bucket = aws_s3_bucket.data_lake.id
  key    = local.glue_transform_script
  source = "${path.module}/../../aws/glue/transform_taxi.py"

  etag                   = filemd5("${path.module}/../../aws/glue/transform_taxi.py")
  content_type           = "text/x-python"
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "transform_taxi" {
  name        = local.glue_transform_job_name
  description = "Transforms one NYC taxi Bronze partition into Silver and Quarantine."
  role_arn    = aws_iam_role.glue_execution.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  execution_class   = "STANDARD"
  timeout           = 30
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/${aws_s3_object.glue_transform_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = ""
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--TAXI_TYPE"                        = "yellow"
    "--YEAR"                             = "2024"
    "--MONTH"                            = "1"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.id}/glue-temp/"
  }

  non_overridable_arguments = {
    "--DATA_LAKE_BUCKET" = aws_s3_bucket.data_lake.id
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_data_lake_access,
  ]
}

output "glue_transform_job_name" {
  description = "Name of the Glue Spark transformation job."
  value       = aws_glue_job.transform_taxi.name
}