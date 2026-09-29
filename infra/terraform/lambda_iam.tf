data "aws_iam_policy_document" "ingestion_lambda_assume_role" {
  statement {
    sid     = "AllowLambdaToAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingestion_lambda" {
  name               = "nyc-taxi-ingestion-lambda-dev"
  description        = "Execution role for the NYC taxi ingestion Lambda."
  assume_role_policy = data.aws_iam_policy_document.ingestion_lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ingestion_lambda_logging" {
  role       = aws_iam_role.ingestion_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "ingestion_lambda_s3_access" {
  statement {
    sid    = "ReadDataLakeBucketMetadata"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
    ]

    resources = [
      aws_s3_bucket.data_lake.arn,
    ]
  }

  statement {
    sid    = "ListBronzePrefix"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]

    resources = [
      aws_s3_bucket.data_lake.arn,
    ]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"

      values = [
        "bronze",
        "bronze/*",
      ]
    }
  }

  statement {
    sid    = "ManageBronzeObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]

    resources = [
      "${aws_s3_bucket.data_lake.arn}/bronze/*",
    ]
  }
}

resource "aws_iam_role_policy" "ingestion_lambda_s3_access" {
  name   = "nyc-taxi-ingestion-bronze-access"
  role   = aws_iam_role.ingestion_lambda.id
  policy = data.aws_iam_policy_document.ingestion_lambda_s3_access.json
}

output "ingestion_lambda_role_arn" {
  description = "ARN of the IAM role assumed by the ingestion Lambda."
  value       = aws_iam_role.ingestion_lambda.arn
}