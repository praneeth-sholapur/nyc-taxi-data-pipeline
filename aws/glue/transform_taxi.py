from __future__ import annotations

import json
import sys
from typing import Any

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REQUIRED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
}

TRIP_ID_COLUMNS = [
    "vendor_id",
    "pickup_at",
    "dropoff_at",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra_amount",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]


def validate_arguments(
    taxi_type: str,
    year: int,
    month: int,
) -> None:
    """Validate one Glue partition request."""
    if taxi_type != "yellow":
        raise ValueError("Only yellow taxi data is currently supported")

    if not 2009 <= year <= 2100:
        raise ValueError("year must be between 2009 and 2100")

    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")


def validate_source_schema(source: DataFrame) -> None:
    """Fail clearly if the Bronze schema is incompatible."""
    missing_columns = sorted(REQUIRED_COLUMNS - set(source.columns))

    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(
            f"Source schema is missing required columns: {missing}"
        )


def normalize_source(source: DataFrame) -> DataFrame:
    """Normalize TLC source columns into the Silver naming convention."""
    return source.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_at"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_at"),
        F.col("passenger_count").cast("long").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("long").alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast("string").alias(
            "store_and_fwd_flag"
        ),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("payment_type").cast("long").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra_amount"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias(
            "improvement_surcharge"
        ),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias(
            "congestion_surcharge"
        ),
        F.col("Airport_fee").cast("double").alias("airport_fee"),
    )


def add_trip_identifiers(normalized: DataFrame) -> DataFrame:
    """Create deterministic MD5 trip IDs from normalized source values."""
    identifier_parts = [
        F.coalesce(
            F.col(column_name).cast("string"),
            F.lit("<NULL>"),
        )
        for column_name in TRIP_ID_COLUMNS
    ]

    return normalized.withColumn(
        "trip_id",
        F.md5(F.concat_ws("|", *identifier_parts)),
    )


def classify_trips(
    source: DataFrame,
    year: int,
    month: int,
) -> DataFrame:
    """Apply duplicate detection and ordered hard-rejection rules."""
    month_start = f"{year}-{month:02d}-01 00:00:00"

    if month == 12:
        next_month_start = f"{year + 1}-01-01 00:00:00"
    else:
        next_month_start = f"{year}-{month + 1:02d}-01 00:00:00"

    normalized = normalize_source(source)
    keyed = add_trip_identifiers(normalized)

    duplicate_window = Window.partitionBy("trip_id").orderBy(
        F.col("pickup_at"),
        F.col("dropoff_at"),
    )

    ranked = keyed.withColumn(
        "duplicate_rank",
        F.row_number().over(duplicate_window),
    )

    duration_seconds = (
        F.col("dropoff_at").cast("long")
        - F.col("pickup_at").cast("long")
    )

    rejection_reason = (
        F.when(
            F.col("duplicate_rank") > 1,
            F.lit("exact_duplicate"),
        )
        .when(
            F.col("pickup_at").isNull()
            | F.col("dropoff_at").isNull(),
            F.lit("missing_timestamp"),
        )
        .when(
            (F.col("pickup_at") < F.lit(month_start).cast("timestamp"))
            | (
                F.col("pickup_at")
                >= F.lit(next_month_start).cast("timestamp")
            ),
            F.lit("pickup_outside_expected_month"),
        )
        .when(
            duration_seconds <= 0,
            F.lit("zero_or_negative_duration"),
        )
        .when(
            duration_seconds > 24 * 60 * 60,
            F.lit("duration_over_24_hours"),
        )
        .when(
            F.col("trip_distance") < 0,
            F.lit("negative_distance"),
        )
        .when(
            ~F.col("pickup_location_id").between(1, 265),
            F.lit("invalid_pickup_zone"),
        )
        .when(
            ~F.col("dropoff_location_id").between(1, 265),
            F.lit("invalid_dropoff_zone"),
        )
    )

    return ranked.withColumn(
        "rejection_reason",
        rejection_reason,
    )


def build_silver(
    classified: DataFrame,
    year: int,
    month: int,
) -> DataFrame:
    """Create accepted, standardized Silver trip records."""
    duration_minutes = F.floor(
        (
            F.col("dropoff_at").cast("long")
            - F.col("pickup_at").cast("long")
        )
        / 60
    ).cast("long")

    return classified.filter(
        F.col("rejection_reason").isNull()
    ).select(
        "trip_id",
        "vendor_id",
        "pickup_at",
        "dropoff_at",
        duration_minutes.alias("duration_minutes"),
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "extra_amount",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        F.col("passenger_count").isNull().alias(
            "passenger_count_missing"
        ),
        (F.col("trip_distance") == 0).alias("zero_distance"),
        (F.col("total_amount") < 0).alias("negative_total_amount"),
        F.lit(year).cast("int").alias("source_year"),
        F.lit(month).cast("int").alias("source_month"),
    )


def build_quarantine(
    classified: DataFrame,
    year: int,
    month: int,
) -> DataFrame:
    """Create rejected records with documented rejection reasons."""
    return classified.filter(
        F.col("rejection_reason").isNotNull()
    ).withColumn(
        "source_year",
        F.lit(year).cast("int"),
    ).withColumn(
        "source_month",
        F.lit(month).cast("int"),
    )


def build_paths(
    bucket: str,
    taxi_type: str,
    year: int,
    month: int,
) -> dict[str, str]:
    """Build partition-specific S3 input and output paths."""
    partition = (
        f"taxi_type={taxi_type}/year={year}/month={month:02d}"
    )
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    return {
        "bronze": f"s3://{bucket}/bronze/{partition}/{filename}",
        "silver": f"s3://{bucket}/silver/{partition}/",
        "quarantine": f"s3://{bucket}/quarantine/{partition}/",
    }


def emit_metrics(metrics: dict[str, Any]) -> None:
    """Write structured execution metrics to CloudWatch logs."""
    print(json.dumps(metrics, sort_keys=True))


def main() -> None:
    """Run one monthly Bronze-to-Silver-and-Quarantine job."""
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "DATA_LAKE_BUCKET",
            "TAXI_TYPE",
            "YEAR",
            "MONTH",
        ],
    )

    bucket = args["DATA_LAKE_BUCKET"]
    taxi_type = args["TAXI_TYPE"].strip().lower()
    year = int(args["YEAR"])
    month = int(args["MONTH"])

    validate_arguments(
        taxi_type=taxi_type,
        year=year,
        month=month,
    )

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    paths = build_paths(
        bucket=bucket,
        taxi_type=taxi_type,
        year=year,
        month=month,
    )

    source = spark.read.parquet(paths["bronze"])
    validate_source_schema(source)

    classified = classify_trips(
        source=source,
        year=year,
        month=month,
    ).persist(StorageLevel.MEMORY_AND_DISK)

    source_rows = classified.count()
    accepted_rows = classified.filter(
        F.col("rejection_reason").isNull()
    ).count()
    quarantined_rows = classified.filter(
        F.col("rejection_reason").isNotNull()
    ).count()
    duplicate_rows = classified.filter(
        F.col("duplicate_rank") > 1
    ).count()

    if accepted_rows + quarantined_rows != source_rows:
        raise RuntimeError(
            "Transformation row reconciliation failed"
        )

    silver = build_silver(
        classified=classified,
        year=year,
        month=month,
    )
    quarantine = build_quarantine(
        classified=classified,
        year=year,
        month=month,
    )

    (
        silver.repartition(8)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(paths["silver"])
    )

    (
        quarantine.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(paths["quarantine"])
    )

    emit_metrics(
        {
            "event": "glue_transformation_completed",
            "partition_key": f"{taxi_type}/{year}/{month:02d}",
            "source_rows": source_rows,
            "accepted_rows": accepted_rows,
            "quarantined_rows": quarantined_rows,
            "duplicate_rows": duplicate_rows,
            "reconciled": (
                accepted_rows + quarantined_rows == source_rows
            ),
            "silver_path": paths["silver"],
            "quarantine_path": paths["quarantine"],
        }
    )

    classified.unpersist()
    job.commit()


if __name__ == "__main__":
    main()