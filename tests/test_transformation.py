from pathlib import Path

import duckdb

from taxi_pipeline.transformation import transform_yellow_partition


def _create_transformation_fixture(path: Path) -> None:
    safe_path = path.resolve().as_posix().replace("'", "''")

    query = """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    TIMESTAMP '2024-01-01 10:00:00',
                    TIMESTAMP '2024-01-01 10:15:00',
                    1,
                    2.5,
                    1,
                    'N',
                    100,
                    110,
                    1,
                    12.0,
                    0.5,
                    0.5,
                    2.0,
                    0.0,
                    1.0,
                    16.0,
                    2.5,
                    0.0
                ),
                (
                    1,
                    TIMESTAMP '2024-01-01 10:00:00',
                    TIMESTAMP '2024-01-01 10:15:00',
                    1,
                    2.5,
                    1,
                    'N',
                    100,
                    110,
                    1,
                    12.0,
                    0.5,
                    0.5,
                    2.0,
                    0.0,
                    1.0,
                    16.0,
                    2.5,
                    0.0
                ),
                (
                    2,
                    TIMESTAMP '2024-01-02 11:00:00',
                    TIMESTAMP '2024-01-02 11:10:00',
                    NULL,
                    0.0,
                    1,
                    'N',
                    101,
                    111,
                    2,
                    5.0,
                    0.0,
                    0.5,
                    0.0,
                    0.0,
                    1.0,
                    -6.5,
                    0.0,
                    0.0
                ),
                (
                    1,
                    TIMESTAMP '2002-12-31 22:59:39',
                    TIMESTAMP '2002-12-31 23:05:41',
                    1,
                    1.0,
                    1,
                    'N',
                    102,
                    112,
                    1,
                    8.0,
                    0.0,
                    0.5,
                    1.0,
                    0.0,
                    1.0,
                    10.5,
                    0.0,
                    0.0
                ),
                (
                    1,
                    TIMESTAMP '2024-01-03 12:00:00',
                    TIMESTAMP '2024-01-03 11:45:00',
                    1,
                    1.5,
                    1,
                    'N',
                    103,
                    113,
                    1,
                    9.0,
                    0.0,
                    0.5,
                    1.5,
                    0.0,
                    1.0,
                    12.0,
                    0.0,
                    0.0
                ),
                (
                    1,
                    TIMESTAMP '2024-01-04 10:00:00',
                    TIMESTAMP '2024-01-05 11:00:00',
                    1,
                    3.0,
                    1,
                    'N',
                    104,
                    114,
                    1,
                    15.0,
                    0.0,
                    0.5,
                    2.0,
                    0.0,
                    1.0,
                    18.5,
                    0.0,
                    0.0
                )
        ) AS trips(
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            Airport_fee
        )
    """

    with duckdb.connect() as connection:
        connection.execute(
            f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)"
        )


def test_transform_yellow_partition_classifies_records(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "bronze.parquet"
    silver_path = tmp_path / "silver" / "trips.parquet"
    quarantine_path = (
        tmp_path / "quarantine" / "rejected.parquet"
    )
    _create_transformation_fixture(source_path)

    result = transform_yellow_partition(
        source_path=source_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
        year=2024,
        month=1,
    )

    assert result.source_rows == 6
    assert result.accepted_rows == 2
    assert result.quarantined_rows == 4
    assert result.duplicate_rows == 1
    assert silver_path.exists()
    assert quarantine_path.exists()

    with duckdb.connect() as connection:
        silver_metrics = connection.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT trip_id) AS distinct_trip_ids,
                SUM(
                    CAST(passenger_count_missing AS INTEGER)
                ) AS missing_passenger_flags,
                SUM(
                    CAST(zero_distance AS INTEGER)
                ) AS zero_distance_flags,
                SUM(
                    CAST(negative_total_amount AS INTEGER)
                ) AS negative_total_flags
            FROM read_parquet(?)
            """,
            [str(silver_path.resolve())],
        ).fetchone()

        rejection_rows = connection.execute(
            """
            SELECT
                rejection_reason,
                COUNT(*) AS row_count
            FROM read_parquet(?)
            GROUP BY rejection_reason
            """,
            [str(quarantine_path.resolve())],
        ).fetchall()

    assert silver_metrics == (2, 2, 1, 1, 1)

    rejection_counts = dict(rejection_rows)
    assert rejection_counts == {
        "duration_over_24_hours": 1,
        "exact_duplicate": 1,
        "pickup_outside_expected_month": 1,
        "zero_or_negative_duration": 1,
    }