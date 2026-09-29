locals {
  data_lake_partition_keys = [
    {
      name = "taxi_type"
      type = "string"
    },
    {
      name = "year"
      type = "string"
    },
    {
      name = "month"
      type = "string"
    },
  ]

  partition_projection_parameters = {
    "EXTERNAL"                    = "TRUE"
    "classification"              = "parquet"
    "projection.enabled"          = "true"
    "projection.taxi_type.type"   = "enum"
    "projection.taxi_type.values" = "yellow"
    "projection.year.type"        = "integer"
    "projection.year.range"       = "2009,2100"
    "projection.month.type"       = "integer"
    "projection.month.range"      = "1,12"
    "projection.month.digits"     = "2"
  }

  silver_columns = [
    { name = "trip_id", type = "string" },
    { name = "vendor_id", type = "int" },
    { name = "pickup_at", type = "timestamp" },
    { name = "dropoff_at", type = "timestamp" },
    { name = "duration_minutes", type = "bigint" },
    { name = "passenger_count", type = "bigint" },
    { name = "trip_distance", type = "double" },
    { name = "rate_code_id", type = "bigint" },
    { name = "store_and_fwd_flag", type = "string" },
    { name = "pickup_location_id", type = "int" },
    { name = "dropoff_location_id", type = "int" },
    { name = "payment_type", type = "bigint" },
    { name = "fare_amount", type = "double" },
    { name = "extra_amount", type = "double" },
    { name = "mta_tax", type = "double" },
    { name = "tip_amount", type = "double" },
    { name = "tolls_amount", type = "double" },
    { name = "improvement_surcharge", type = "double" },
    { name = "total_amount", type = "double" },
    { name = "congestion_surcharge", type = "double" },
    { name = "airport_fee", type = "double" },
    { name = "passenger_count_missing", type = "boolean" },
    { name = "zero_distance", type = "boolean" },
    { name = "negative_total_amount", type = "boolean" },
    { name = "source_year", type = "int" },
    { name = "source_month", type = "int" },
  ]

  quarantine_columns = [
    { name = "vendor_id", type = "int" },
    { name = "pickup_at", type = "timestamp" },
    { name = "dropoff_at", type = "timestamp" },
    { name = "passenger_count", type = "bigint" },
    { name = "trip_distance", type = "double" },
    { name = "rate_code_id", type = "bigint" },
    { name = "store_and_fwd_flag", type = "string" },
    { name = "pickup_location_id", type = "int" },
    { name = "dropoff_location_id", type = "int" },
    { name = "payment_type", type = "bigint" },
    { name = "fare_amount", type = "double" },
    { name = "extra_amount", type = "double" },
    { name = "mta_tax", type = "double" },
    { name = "tip_amount", type = "double" },
    { name = "tolls_amount", type = "double" },
    { name = "improvement_surcharge", type = "double" },
    { name = "total_amount", type = "double" },
    { name = "congestion_surcharge", type = "double" },
    { name = "airport_fee", type = "double" },
    { name = "trip_id", type = "string" },
    { name = "duplicate_rank", type = "int" },
    { name = "rejection_reason", type = "string" },
    { name = "source_year", type = "int" },
    { name = "source_month", type = "int" },
  ]
}

resource "aws_glue_catalog_table" "silver_trips" {
  database_name = aws_glue_catalog_database.nyc_taxi.name
  name          = "silver_trips"
  description   = "Validated and standardized NYC Yellow Taxi trips."
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    local.partition_projection_parameters,
    {
      "storage.location.template" = "s3://${aws_s3_bucket.data_lake.id}/silver/taxi_type=$${taxi_type}/year=$${year}/month=$${month}/"
    }
  )

  dynamic "partition_keys" {
    for_each = local.data_lake_partition_keys

    content {
      name = partition_keys.value.name
      type = partition_keys.value.type
    }
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/silver/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    dynamic "columns" {
      for_each = local.silver_columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }

    ser_de_info {
      name                  = "silver-trips-parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
  }
}

resource "aws_glue_catalog_table" "quarantine_trips" {
  database_name = aws_glue_catalog_database.nyc_taxi.name
  name          = "quarantine_trips"
  description   = "Rejected NYC Yellow Taxi records with rejection reasons."
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    local.partition_projection_parameters,
    {
      "storage.location.template" = "s3://${aws_s3_bucket.data_lake.id}/quarantine/taxi_type=$${taxi_type}/year=$${year}/month=$${month}/"
    }
  )

  dynamic "partition_keys" {
    for_each = local.data_lake_partition_keys

    content {
      name = partition_keys.value.name
      type = partition_keys.value.type
    }
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/quarantine/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    dynamic "columns" {
      for_each = local.quarantine_columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }

    ser_de_info {
      name                  = "quarantine-trips-parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
  }
}

output "silver_table_name" {
  description = "Glue Catalog table for validated Silver trips."
  value       = aws_glue_catalog_table.silver_trips.name
}

output "quarantine_table_name" {
  description = "Glue Catalog table for rejected trips."
  value       = aws_glue_catalog_table.quarantine_trips.name
}