from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class QualityReport:
    """Automated quality results for one transformed partition."""

    source_rows: int
    silver_rows: int
    quarantine_rows: int
    reconciled: bool
    duplicate_trip_ids: int
    invalid_silver_rows: int
    missing_passenger_flags: int
    zero_distance_flags: int
    negative_total_flags: int
    rejection_counts: dict[str, int]
    passed: bool


def validate_partition_outputs(
    source_path: Path,
    silver_path: Path,
    quarantine_path: Path,
    year: int,
    month: int,
) -> QualityReport:
    """Validate reconciliation and silver-layer quality rules."""
    required_paths = [
        source_path,
        silver_path,
        quarantine_path,
    ]
    missing_paths = [
        str(path)
        for path in required_paths
        if not path.exists()
    ]

    if missing_paths:
        missing = ", ".join(missing_paths)
        raise FileNotFoundError(
            f"Required pipeline outputs do not exist: {missing}"
        )

    with duckdb.connect() as connection:
        source_result = connection.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(source_path.resolve())],
        ).fetchone()

        silver_metrics = connection.execute(
            """
            WITH bounds AS (
                SELECT make_date(?, ?, 1) AS month_start
            )
            SELECT
                COUNT(*) AS silver_rows,

                COUNT(*) - COUNT(DISTINCT trip_id)
                    AS duplicate_trip_ids,

                COUNT(*) FILTER (
                    WHERE pickup_at IS NULL
                       OR dropoff_at IS NULL
                       OR pickup_at < month_start
                       OR pickup_at
                            >= month_start + INTERVAL '1 month'
                       OR dropoff_at <= pickup_at
                       OR dropoff_at - pickup_at
                            > INTERVAL '24 hours'
                       OR trip_distance < 0
                       OR pickup_location_id
                            NOT BETWEEN 1 AND 265
                       OR dropoff_location_id
                            NOT BETWEEN 1 AND 265
                ) AS invalid_silver_rows,

                COALESCE(
                    SUM(
                        CAST(
                            passenger_count_missing AS INTEGER
                        )
                    ),
                    0
                ) AS missing_passenger_flags,

                COALESCE(
                    SUM(CAST(zero_distance AS INTEGER)),
                    0
                ) AS zero_distance_flags,

                COALESCE(
                    SUM(
                        CAST(
                            negative_total_amount AS INTEGER
                        )
                    ),
                    0
                ) AS negative_total_flags

            FROM read_parquet(?)
            CROSS JOIN bounds
            """,
            [
                year,
                month,
                str(silver_path.resolve()),
            ],
        ).fetchone()

        rejection_rows = connection.execute(
            """
            SELECT
                rejection_reason,
                COUNT(*) AS row_count
            FROM read_parquet(?)
            GROUP BY rejection_reason
            ORDER BY rejection_reason
            """,
            [str(quarantine_path.resolve())],
        ).fetchall()

    if source_result is None or silver_metrics is None:
        raise RuntimeError(
            "Quality validation did not return expected metrics"
        )

    source_rows = int(source_result[0])
    silver_rows = int(silver_metrics[0])
    duplicate_trip_ids = int(silver_metrics[1])
    invalid_silver_rows = int(silver_metrics[2])
    missing_passenger_flags = int(silver_metrics[3])
    zero_distance_flags = int(silver_metrics[4])
    negative_total_flags = int(silver_metrics[5])

    rejection_counts = {
        str(reason): int(row_count)
        for reason, row_count in rejection_rows
    }
    quarantine_rows = sum(rejection_counts.values())

    reconciled = (
        silver_rows + quarantine_rows == source_rows
    )
    passed = (
        reconciled
        and duplicate_trip_ids == 0
        and invalid_silver_rows == 0
    )

    return QualityReport(
        source_rows=source_rows,
        silver_rows=silver_rows,
        quarantine_rows=quarantine_rows,
        reconciled=reconciled,
        duplicate_trip_ids=duplicate_trip_ids,
        invalid_silver_rows=invalid_silver_rows,
        missing_passenger_flags=missing_passenger_flags,
        zero_distance_flags=zero_distance_flags,
        negative_total_flags=negative_total_flags,
        rejection_counts=rejection_counts,
        passed=passed,
    )