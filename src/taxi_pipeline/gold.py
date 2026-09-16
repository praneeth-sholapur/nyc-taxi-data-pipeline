from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class GoldBuildResult:
    source_rows: int
    gold_rows: int
    total_trip_count: int
    output_path: Path


def build_gold_partition(
    data_root: Path,
    taxi_type: str,
    year: int,
    month: int,
) -> GoldBuildResult:
    source_path = (
        data_root
        / "silver"
        / f"taxi_type={taxi_type}"
        / f"year={year}"
        / f"month={month:02d}"
        / "trips.parquet"
    )
    output_path = (
        data_root
        / "gold"
        / "pickup_zone_hourly"
        / f"taxi_type={taxi_type}"
        / f"year={year}"
        / f"month={month:02d}"
        / "metrics.parquet"
    )

    if not source_path.is_file():
        raise FileNotFoundError(f"Silver partition does not exist: {source_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f".{output_path.name}.tmp")
    temporary_path.unlink(missing_ok=True)

    try:
        with duckdb.connect() as connection:
            connection.read_parquet(str(source_path)).create_view("silver_partition")

            source_rows = connection.execute(
                "SELECT COUNT(*) FROM silver_partition"
            ).fetchone()[0]

            gold_relation = connection.sql(
                """
                SELECT
                    CAST(pickup_at AS DATE) AS service_date,
                    CAST(EXTRACT(HOUR FROM pickup_at) AS INTEGER) AS pickup_hour,
                    pickup_location_id,
                    COUNT(*)::BIGINT AS trip_count,
                    SUM(COALESCE(passenger_count, 0))::BIGINT
                        AS passenger_count_total,
                    ROUND(SUM(trip_distance), 3) AS trip_distance_total,
                    ROUND(SUM(fare_amount), 2) AS fare_amount_total,
                    ROUND(SUM(tip_amount), 2) AS tip_amount_total,
                    ROUND(SUM(total_amount), 2) AS total_amount_net,
                    ROUND(AVG(duration_minutes), 2) AS duration_minutes_average,
                    ROUND(AVG(trip_distance), 3) AS trip_distance_average,
                    ROUND(AVG(total_amount), 2) AS total_amount_average,
                    SUM(
                        CAST(COALESCE(passenger_count_missing, FALSE) AS BIGINT)
                    )::BIGINT AS missing_passenger_count,
                    SUM(
                        CAST(COALESCE(zero_distance, FALSE) AS BIGINT)
                    )::BIGINT AS zero_distance_count,
                    SUM(
                        CAST(COALESCE(negative_total_amount, FALSE) AS BIGINT)
                    )::BIGINT AS negative_total_amount_count
                FROM silver_partition
                GROUP BY
                    service_date,
                    pickup_hour,
                    pickup_location_id
                ORDER BY
                    service_date,
                    pickup_hour,
                    pickup_location_id
                """
            )
            gold_relation.write_parquet(
                str(temporary_path),
                compression="zstd",
            )

            gold_rows, total_trip_count = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(trip_count), 0)
                FROM read_parquet(?)
                """,
                [str(temporary_path)],
            ).fetchone()

            duplicate_grains = connection.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT
                        service_date,
                        pickup_hour,
                        pickup_location_id
                    FROM read_parquet(?)
                    GROUP BY
                        service_date,
                        pickup_hour,
                        pickup_location_id
                    HAVING COUNT(*) > 1
                )
                """,
                [str(temporary_path)],
            ).fetchone()[0]

            invalid_counts = connection.execute(
                """
                SELECT COUNT(*)
                FROM read_parquet(?)
                WHERE trip_count <= 0
                """,
                [str(temporary_path)],
            ).fetchone()[0]

            if total_trip_count != source_rows:
                raise RuntimeError(
                    "Gold trip counts do not reconcile with the Silver source: "
                    f"{total_trip_count} != {source_rows}"
                )

            if duplicate_grains != 0:
                raise RuntimeError(
                    f"Gold output contains {duplicate_grains} duplicate grains"
                )

            if invalid_counts != 0:
                raise RuntimeError(
                    f"Gold output contains {invalid_counts} invalid trip counts"
                )

        temporary_path.replace(output_path)

        return GoldBuildResult(
            source_rows=source_rows,
            gold_rows=gold_rows,
            total_trip_count=total_trip_count,
            output_path=output_path,
        )
    finally:
        temporary_path.unlink(missing_ok=True)