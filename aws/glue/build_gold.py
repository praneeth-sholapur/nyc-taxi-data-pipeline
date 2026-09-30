from __future__ import annotations

import json
import sys
from typing import Any

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_SILVER_COLUMNS = {
    "pickup_at",
    "pickup_location_id",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "duration_minutes",
    "passenger_count_missing",
    "zero_distance",
    "negative_total_amount",
}


def validate_arguments(
    taxi_type: str,
    year: int,
    month: int,
) -> None:
    """Validate one monthly Gold-build request."""
    if taxi_type != "yellow":
        raise ValueError("Only yellow taxi data is currently supported")

    if not 2009 <= year <= 2100:
        raise ValueError("year must be between 2009 and 2100")

    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")


def validate_silver_schema(source: DataFrame) -> None:
    """Fail clearly if the Silver schema is incompatible."""
    missing_columns = sorted(
        REQUIRED_SILVER_COLUMNS - set(source.columns)
    )

    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(
            f"Silver schema is missing required columns: {missing}"
        )


def build_paths(
    bucket: str,
    taxi_type: str,
    year: int,
    month: int,
) -> dict[str, str]:
    """Build partition-specific Silver and Gold S3 paths."""
    partition = (
        f"taxi_type={taxi_type}/year={year}/month={month:02d}"
    )

    return {
        "silver": f"s3://{bucket}/silver/{partition}/",
        "gold": (
            f"s3://{bucket}/gold/pickup_zone_hourly/"
            f"{partition}/"
        ),
    }


def build_gold(source: DataFrame) -> DataFrame:
    """Aggregate Silver trips to pickup-zone hourly metrics."""
    missing_passenger_count = F.sum(
        F.when(
            F.coalesce(
                F.col("passenger_count_missing"),
                F.lit(False),
            ),
            F.lit(1),
        ).otherwise(F.lit(0))
    ).cast("long")

    zero_distance_count = F.sum(
        F.when(
            F.coalesce(
                F.col("zero_distance"),
                F.lit(False),
            ),
            F.lit(1),
        ).otherwise(F.lit(0))
    ).cast("long")

    negative_total_amount_count = F.sum(
        F.when(
            F.coalesce(
                F.col("negative_total_amount"),
                F.lit(False),
            ),
            F.lit(1),
        ).otherwise(F.lit(0))
    ).cast("long")

    return (
        source.withColumn(
            "service_date",
            F.to_date(F.col("pickup_at")),
        )
        .withColumn(
            "pickup_hour",
            F.hour(F.col("pickup_at")).cast("int"),
        )
        .groupBy(
            "service_date",
            "pickup_hour",
            "pickup_location_id",
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("trip_count"),
            F.sum(
                F.coalesce(
                    F.col("passenger_count"),
                    F.lit(0),
                )
            )
            .cast("long")
            .alias("passenger_count_total"),
            F.round(
                F.sum(F.col("trip_distance")),
                3,
            ).alias("trip_distance_total"),
            F.round(
                F.sum(F.col("fare_amount")),
                2,
            ).alias("fare_amount_total"),
            F.round(
                F.sum(F.col("tip_amount")),
                2,
            ).alias("tip_amount_total"),
            F.round(
                F.sum(F.col("total_amount")),
                2,
            ).alias("total_amount_net"),
            F.round(
                F.avg(F.col("duration_minutes")),
                2,
            ).alias("duration_minutes_average"),
            F.round(
                F.avg(F.col("trip_distance")),
                3,
            ).alias("trip_distance_average"),
            F.round(
                F.avg(F.col("total_amount")),
                2,
            ).alias("total_amount_average"),
            missing_passenger_count.alias(
                "missing_passenger_count"
            ),
            zero_distance_count.alias("zero_distance_count"),
            negative_total_amount_count.alias(
                "negative_total_amount_count"
            ),
        )
    )


def validate_gold(
    gold: DataFrame,
    source_rows: int,
) -> dict[str, int]:
    """Validate Gold grain, counts and Silver reconciliation."""
    gold_rows = gold.count()

    trip_count_row = gold.agg(
        F.coalesce(
            F.sum(F.col("trip_count")),
            F.lit(0),
        ).cast("long").alias("total_trip_count")
    ).first()

    total_trip_count = int(trip_count_row["total_trip_count"])

    duplicate_grains = (
        gold.groupBy(
            "service_date",
            "pickup_hour",
            "pickup_location_id",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    invalid_counts = gold.filter(
        F.col("trip_count") <= 0
    ).count()

    if total_trip_count != source_rows:
        raise RuntimeError(
            "Gold trip counts do not reconcile with Silver: "
            f"{total_trip_count} != {source_rows}"
        )

    if duplicate_grains != 0:
        raise RuntimeError(
            f"Gold contains {duplicate_grains} duplicate grains"
        )

    if invalid_counts != 0:
        raise RuntimeError(
            f"Gold contains {invalid_counts} invalid trip counts"
        )

    return {
        "gold_rows": gold_rows,
        "total_trip_count": total_trip_count,
        "duplicate_grains": duplicate_grains,
        "invalid_counts": invalid_counts,
    }


def emit_metrics(metrics: dict[str, Any]) -> None:
    """Write structured execution metrics to CloudWatch logs."""
    print(json.dumps(metrics, sort_keys=True))


def main() -> None:
    """Run one monthly Silver-to-Gold aggregation job."""
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

    source = spark.read.parquet(paths["silver"])
    validate_silver_schema(source)

    source_rows = source.count()

    gold = build_gold(source).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    validation = validate_gold(
        gold=gold,
        source_rows=source_rows,
    )

    (
        gold.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(paths["gold"])
    )

    emit_metrics(
        {
            "event": "glue_gold_build_completed",
            "partition_key": f"{taxi_type}/{year}/{month:02d}",
            "source_rows": source_rows,
            "gold_rows": validation["gold_rows"],
            "total_trip_count": validation["total_trip_count"],
            "duplicate_grains": validation["duplicate_grains"],
            "invalid_counts": validation["invalid_counts"],
            "reconciled": (
                validation["total_trip_count"] == source_rows
            ),
            "gold_path": paths["gold"],
        }
    )

    gold.unpersist()
    job.commit()


if __name__ == "__main__":
    main()