from __future__ import annotations

from pathlib import Path

import duckdb


def get_parquet_schema(path: Path) -> list[tuple[str, str]]:
    """Return column names and DuckDB data types for a Parquet file."""
    with duckdb.connect() as connection:
        rows = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(path.resolve())],
        ).fetchall()

    return [
        (str(row[0]), str(row[1]))
        for row in rows
    ]

def count_exact_duplicate_rows(path: Path) -> int:
    """Count additional copies of completely identical source rows."""
    query = """
        SELECT COALESCE(SUM(occurrences - 1), 0)
        FROM (
            SELECT
                *,
                COUNT(*) AS occurrences
            FROM read_parquet(?)
            GROUP BY ALL
            HAVING COUNT(*) > 1
        )
    """

    with duckdb.connect() as connection:
        result = connection.execute(
            query,
            [str(path.resolve())],
        ).fetchone()

    return int(result[0]) if result else 0

def profile_yellow_trips(
    path: Path,
    year: int,
    month: int,
) -> dict[str, object]:
    """Calculate source-quality metrics for Yellow Taxi records."""
    query = """
        WITH bounds AS (
            SELECT make_date(?, ?, 1) AS month_start
        ),
        trips AS (
            SELECT *
            FROM read_parquet(?)
        )
        SELECT
            COUNT(*) AS row_count,
            MIN(tpep_pickup_datetime) AS minimum_pickup_at,
            MAX(tpep_pickup_datetime) AS maximum_pickup_at,
            MIN(tpep_dropoff_datetime) AS minimum_dropoff_at,
            MAX(tpep_dropoff_datetime) AS maximum_dropoff_at,

            COUNT(*) FILTER (
                WHERE tpep_pickup_datetime IS NULL
            ) AS missing_pickup_timestamps,

            COUNT(*) FILTER (
                WHERE tpep_dropoff_datetime IS NULL
            ) AS missing_dropoff_timestamps,

            COUNT(*) FILTER (
                WHERE tpep_dropoff_datetime < tpep_pickup_datetime
            ) AS dropoff_before_pickup,

            COUNT(*) FILTER (
                WHERE tpep_dropoff_datetime <= tpep_pickup_datetime
            ) AS zero_or_negative_duration,

            COUNT(*) FILTER (
                WHERE tpep_dropoff_datetime - tpep_pickup_datetime
                    > INTERVAL '24 hours'
            ) AS duration_over_24_hours,

            COUNT(*) FILTER (
                WHERE tpep_pickup_datetime < month_start
                   OR tpep_pickup_datetime
                        >= month_start + INTERVAL '1 month'
            ) AS pickup_outside_expected_month,

            COUNT(*) FILTER (
                WHERE passenger_count IS NULL
            ) AS missing_passenger_count,

            COUNT(*) FILTER (
                WHERE trip_distance < 0
            ) AS negative_trip_distance,

            COUNT(*) FILTER (
                WHERE trip_distance = 0
            ) AS zero_trip_distance,

            COUNT(*) FILTER (
                WHERE PULocationID NOT BETWEEN 1 AND 265
            ) AS invalid_pickup_zone,

            COUNT(*) FILTER (
                WHERE DOLocationID NOT BETWEEN 1 AND 265
            ) AS invalid_dropoff_zone,

            COUNT(*) FILTER (
                WHERE total_amount < 0
            ) AS negative_total_amount,

            ROUND(AVG(trip_distance), 3) AS average_trip_distance,
            ROUND(AVG(total_amount), 2) AS average_total_amount
        FROM trips
        CROSS JOIN bounds
    """

    with duckdb.connect() as connection:
        cursor = connection.execute(
            query,
            [
                year,
                month,
                str(path.resolve()),
            ],
        )
        row = cursor.fetchone()
        column_names = [
            description[0]
            for description in cursor.description
        ]

    if row is None:
        raise ValueError(f"Could not profile Parquet file: {path}")

    return dict(zip(column_names, row, strict=True))