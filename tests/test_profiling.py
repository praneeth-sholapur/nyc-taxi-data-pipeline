from datetime import datetime
from pathlib import Path

import duckdb

from taxi_pipeline.profiling import (
    count_exact_duplicate_rows,
    get_parquet_schema,
    profile_yellow_trips,
)

def test_count_exact_duplicate_rows(tmp_path: Path) -> None:
    parquet_path = tmp_path / "duplicates.parquet"
    safe_path = parquet_path.resolve().as_posix().replace("'", "''")

    query = """
        SELECT *
        FROM (
            VALUES
                (1, 'card'),
                (1, 'card'),
                (2, 'cash')
        ) AS trips(trip_id, payment_method)
    """

    with duckdb.connect() as connection:
        connection.execute(
            f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)"
        )

    assert count_exact_duplicate_rows(parquet_path) == 1

def _create_profile_fixture(path: Path) -> None:
    safe_path = path.resolve().as_posix().replace("'", "''")

    query = """
        SELECT *
        FROM (
            VALUES
                (
                    TIMESTAMP '2024-01-01 10:00:00',
                    TIMESTAMP '2024-01-01 10:15:00',
                    1,
                    2.5,
                    100,
                    110,
                    15.0
                ),
                (
                    TIMESTAMP '2024-01-02 11:00:00',
                    TIMESTAMP '2024-01-02 11:10:00',
                    NULL,
                    0.0,
                    101,
                    111,
                    5.0
                ),
                (
                    TIMESTAMP '2024-01-03 12:00:00',
                    TIMESTAMP '2024-01-03 11:45:00',
                    1,
                    -1.0,
                    999,
                    0,
                    -10.0
                ),
                (
                    NULL,
                    NULL,
                    1,
                    1.0,
                    102,
                    112,
                    10.0
                )
        ) AS trips(
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            PULocationID,
            DOLocationID,
            total_amount
        )
    """

    with duckdb.connect() as connection:
        connection.execute(
            f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)"
        )


def test_get_parquet_schema_returns_columns(tmp_path: Path) -> None:
    parquet_path = tmp_path / "profile.parquet"
    _create_profile_fixture(parquet_path)

    schema = get_parquet_schema(parquet_path)

    assert ("tpep_pickup_datetime", "TIMESTAMP") in schema
    assert ("trip_distance", "DECIMAL(2,1)") in schema
    assert ("PULocationID", "INTEGER") in schema


def test_profile_yellow_trips_calculates_quality_metrics(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "profile.parquet"
    _create_profile_fixture(parquet_path)

    profile = profile_yellow_trips(
    parquet_path,
    year=2024,
    month=1,
)

    assert profile["row_count"] == 4
    assert profile["minimum_pickup_at"] == datetime(2024, 1, 1, 10, 0)
    assert profile["maximum_pickup_at"] == datetime(2024, 1, 3, 12, 0)
    assert profile["missing_pickup_timestamps"] == 1
    assert profile["missing_dropoff_timestamps"] == 1
    assert profile["dropoff_before_pickup"] == 1
    assert profile["missing_passenger_count"] == 1
    assert profile["negative_trip_distance"] == 1
    assert profile["zero_trip_distance"] == 1
    assert profile["invalid_pickup_zone"] == 1
    assert profile["invalid_dropoff_zone"] == 1
    assert profile["negative_total_amount"] == 1
    assert profile["average_trip_distance"] == 0.625
    assert profile["average_total_amount"] == 5.0
    assert profile["zero_or_negative_duration"] == 1
    assert profile["duration_over_24_hours"] == 0
    assert profile["pickup_outside_expected_month"] == 0