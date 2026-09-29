data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    sid     = "AllowGlueToAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_execution" {
  name               = "nyc-taxi-glue-execution-dev"
  description        = "Execution role for the NYC taxi AWS Glue pipeline."
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data_lake_access" {
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
    sid    = "ListApprovedDataLakePrefixes"
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
        "silver",
        "silver/*",
        "quarantine",
        "quarantine/*",
        "gold",
        "gold/*",
        "reference",
        "reference/*",
        "glue-scripts",
        "glue-scripts/*",
        "glue-temp",
        "glue-temp/*",
      ]
    }
  }

  statement {
    sid    = "AccessApprovedDataLakeObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]

    resources = [
      "${aws_s3_bucket.data_lake.arn}/bronze/*",
      "${aws_s3_bucket.data_lake.arn}/silver/*",
      "${aws_s3_bucket.data_lake.arn}/quarantine/*",
      "${aws_s3_bucket.data_lake.arn}/gold/*",
      "${aws_s3_bucket.data_lake.arn}/reference/*",
      "${aws_s3_bucket.data_lake.arn}/glue-scripts/*",
      "${aws_s3_bucket.data_lake.arn}/glue-temp/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_data_lake_access" {
  name   = "nyc-taxi-glue-data-lake-access"
  role   = aws_iam_role.glue_execution.id
  policy = data.aws_iam_policy_document.glue_data_lake_access.json
}

output "glue_execution_role_arn" {
  description = "ARN of the IAM role assumed by AWS Glue jobs and crawlers."
  value       = aws_iam_role.glue_execution.arn
}