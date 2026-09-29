locals {
  ingestion_lambda_name = "nyc-taxi-ingest-partition-dev"
}

data "archive_file" "ingestion_lambda" {
  type        = "zip"
  source_file = "${path.module}/../../src/taxi_pipeline/aws_ingestion.py"
  output_path = "${path.module}/.terraform/aws_ingestion.zip"
}

resource "aws_cloudwatch_log_group" "ingestion_lambda" {
  name              = "/aws/lambda/${local.ingestion_lambda_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "ingestion" {
  function_name = local.ingestion_lambda_name
  description   = "Downloads and validates one NYC TLC partition into S3 Bronze."
  role          = aws_iam_role.ingestion_lambda.arn
  handler       = "aws_ingestion.handler"
  runtime       = "python3.12"
  architectures = ["arm64"]

  filename         = data.archive_file.ingestion_lambda.output_path
  source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256

  memory_size = 1024
  timeout     = 900

  ephemeral_storage {
    size = 1024
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = aws_s3_bucket.data_lake.id
    }
  }

  logging_config {
    log_format            = "JSON"
    application_log_level = "INFO"
    system_log_level      = "INFO"
  }

  depends_on = [
    aws_cloudwatch_log_group.ingestion_lambda,
    aws_iam_role_policy_attachment.ingestion_lambda_logging,
    aws_iam_role_policy.ingestion_lambda_s3_access,
  ]
}

output "ingestion_lambda_function_name" {
  description = "Name of the Lambda function that ingests TLC partitions."
  value       = aws_lambda_function.ingestion.function_name
}