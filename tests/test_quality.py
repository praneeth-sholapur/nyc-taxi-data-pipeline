from pathlib import Path

import duckdb

from taxi_pipeline.quality import validate_partition_outputs


def _write_parquet(path: Path, query: str) -> None:
    safe_path = path.resolve().as_posix().replace("'", "''")
    path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect() as connection:
        connection.execute(
            f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)"
        )


def _create_quality_fixture(
    root: Path,
    duplicate_trip_id: bool = False,
) -> tuple[Path, Path, Path]:
    source_path = root / "bronze.parquet"
    silver_path = root / "silver.parquet"
    quarantine_path = root / "quarantine.parquet"

    _write_parquet(
        source_path,
        """
        SELECT *
        FROM (VALUES (1), (2), (3)) AS source(source_id)
        """,
    )

    second_trip_id = (
        "trip-1"
        if duplicate_trip_id
        else "trip-2"
    )

    _write_parquet(
        silver_path,
        f"""
        SELECT *
        FROM (
            VALUES
                (
                    'trip-1',
                    TIMESTAMP '2024-01-01 10:00:00',
                    TIMESTAMP '2024-01-01 10:15:00',
                    2.5,
                    100,
                    110,
                    FALSE,
                    FALSE,
                    FALSE
                ),
                (
                    '{second_trip_id}',
                    TIMESTAMP '2024-01-02 11:00:00',
                    TIMESTAMP '2024-01-02 11:10:00',
                    0.0,
                    101,
                    111,
                    TRUE,
                    TRUE,
                    TRUE
                )
        ) AS trips(
            trip_id,
            pickup_at,
            dropoff_at,
            trip_distance,
            pickup_location_id,
            dropoff_location_id,
            passenger_count_missing,
            zero_distance,
            negative_total_amount
        )
        """,
    )

    _write_parquet(
        quarantine_path,
        """
        SELECT
            'zero_or_negative_duration' AS rejection_reason
        """,
    )

    return source_path, silver_path, quarantine_path


def test_quality_report_passes_valid_outputs(
    tmp_path: Path,
) -> None:
    source, silver, quarantine = _create_quality_fixture(
        tmp_path
    )

    report = validate_partition_outputs(
        source_path=source,
        silver_path=silver,
        quarantine_path=quarantine,
        year=2024,
        month=1,
    )

    assert report.source_rows == 3
    assert report.silver_rows == 2
    assert report.quarantine_rows == 1
    assert report.reconciled is True
    assert report.duplicate_trip_ids == 0
    assert report.invalid_silver_rows == 0
    assert report.missing_passenger_flags == 1
    assert report.zero_distance_flags == 1
    assert report.negative_total_flags == 1
    assert report.rejection_counts == {
        "zero_or_negative_duration": 1
    }
    assert report.passed is True


def test_quality_report_fails_duplicate_trip_ids(
    tmp_path: Path,
) -> None:
    source, silver, quarantine = _create_quality_fixture(
        tmp_path,
        duplicate_trip_id=True,
    )

    report = validate_partition_outputs(
        source_path=source,
        silver_path=silver,
        quarantine_path=quarantine,
        year=2024,
        month=1,
    )

    assert report.reconciled is True
    assert report.duplicate_trip_ids == 1
    assert report.passed is False