from pathlib import Path

import duckdb
import pytest

from taxi_pipeline.gold import build_gold_partition


def _write_silver_partition(data_root: Path) -> Path:
    source_path = (
        data_root
        / "silver"
        / "taxi_type=yellow"
        / "year=2024"
        / "month=01"
        / "trips.parquet"
    )
    source_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect() as connection:
        connection.sql(
            """
            SELECT *
            FROM (
                VALUES
                    (
                        'trip-1',
                        TIMESTAMP '2024-01-01 08:05:00',
                        10,
                        1::BIGINT,
                        2.0::DOUBLE,
                        10.0::DOUBLE,
                        2.0::DOUBLE,
                        15.0::DOUBLE,
                        15::BIGINT,
                        FALSE,
                        FALSE,
                        FALSE
                    ),
                    (
                        'trip-2',
                        TIMESTAMP '2024-01-01 08:35:00',
                        10,
                        NULL::BIGINT,
                        0.0::DOUBLE,
                        8.0::DOUBLE,
                        0.0::DOUBLE,
                        10.0::DOUBLE,
                        10::BIGINT,
                        TRUE,
                        TRUE,
                        FALSE
                    ),
                    (
                        'trip-3',
                        TIMESTAMP '2024-01-01 09:10:00',
                        20,
                        2::BIGINT,
                        3.0::DOUBLE,
                        12.0::DOUBLE,
                        1.0::DOUBLE,
                        -5.0::DOUBLE,
                        20::BIGINT,
                        FALSE,
                        FALSE,
                        TRUE
           
                    )
            ) AS trips(
                trip_id,
                pickup_at,
                pickup_location_id,
                passenger_count,
                trip_distance,
                fare_amount,
                tip_amount,
                total_amount,
                duration_minutes,
                passenger_count_missing,
                zero_distance,
                negative_total_amount
            )
            """
        ).write_parquet(str(source_path))

    return source_path


def test_build_gold_partition_aggregates_and_reconciles(
    tmp_path: Path,
) -> None:
    _write_silver_partition(tmp_path)

    result = build_gold_partition(
        data_root=tmp_path,
        taxi_type="yellow",
        year=2024,
        month=1,
    )

    assert result.source_rows == 3
    assert result.gold_rows == 2
    assert result.total_trip_count == 3
    assert result.output_path.is_file()

    with duckdb.connect() as connection:
        rows = connection.execute(
            """
            SELECT
                pickup_hour,
                pickup_location_id,
                trip_count,
                passenger_count_total,
                trip_distance_total,
                fare_amount_total,
                tip_amount_total,
                total_amount_net,
                missing_passenger_count,
                zero_distance_count,
                negative_total_amount_count
            FROM read_parquet(?)
            ORDER BY pickup_hour, pickup_location_id
            """,
            [str(result.output_path)],
        ).fetchall()

    assert rows == [
        (8, 10, 2, 1, 2.0, 18.0, 2.0, 25.0, 1, 1, 0),
        (9, 20, 1, 2, 3.0, 12.0, 1.0, -5.0, 0, 0, 1),
    ]


def test_build_gold_partition_is_repeatable(tmp_path: Path) -> None:
    _write_silver_partition(tmp_path)

    first_result = build_gold_partition(tmp_path, "yellow", 2024, 1)
    second_result = build_gold_partition(tmp_path, "yellow", 2024, 1)

    assert first_result.source_rows == second_result.source_rows
    assert first_result.gold_rows == second_result.gold_rows
    assert first_result.total_trip_count == second_result.total_trip_count
    assert first_result.output_path == second_result.output_path


def test_build_gold_partition_requires_silver_source(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError, match="Silver partition does not exist"):
        build_gold_partition(tmp_path, "yellow", 2024, 1)