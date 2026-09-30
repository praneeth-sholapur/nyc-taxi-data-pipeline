locals {
  gold_pickup_zone_hourly_columns = [
    { name = "service_date", type = "date" },
    { name = "pickup_hour", type = "int" },
    { name = "pickup_location_id", type = "int" },
    { name = "trip_count", type = "bigint" },
    { name = "passenger_count_total", type = "bigint" },
    { name = "trip_distance_total", type = "double" },
    { name = "fare_amount_total", type = "double" },
    { name = "tip_amount_total", type = "double" },
    { name = "total_amount_net", type = "double" },
    { name = "duration_minutes_average", type = "double" },
    { name = "trip_distance_average", type = "double" },
    { name = "total_amount_average", type = "double" },
    { name = "missing_passenger_count", type = "bigint" },
    { name = "zero_distance_count", type = "bigint" },
    { name = "negative_total_amount_count", type = "bigint" },
  ]
}

resource "aws_glue_catalog_table" "gold_pickup_zone_hourly" {
  database_name = aws_glue_catalog_database.nyc_taxi.name
  name          = "gold_pickup_zone_hourly"
  description   = "Hourly pickup-zone metrics built from validated Silver trips."
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(
    local.partition_projection_parameters,
    {
      "storage.location.template" = "s3://${aws_s3_bucket.data_lake.id}/gold/pickup_zone_hourly/taxi_type=$${taxi_type}/year=$${year}/month=$${month}/"
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
    location      = "s3://${aws_s3_bucket.data_lake.id}/gold/pickup_zone_hourly/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    dynamic "columns" {
      for_each = local.gold_pickup_zone_hourly_columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }

    ser_de_info {
      name                  = "gold-pickup-zone-hourly-parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
  }
}

output "gold_pickup_zone_hourly_table_name" {
  description = "Glue Catalog table for hourly pickup-zone Gold metrics."
  value       = aws_glue_catalog_table.gold_pickup_zone_hourly.name
}