from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import duckdb


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


@dataclass(frozen=True)
class TransformationResult:
    """Verified row counts and output paths for one transformation."""

    source_rows: int
    accepted_rows: int
    quarantined_rows: int
    duplicate_rows: int
    silver_path: Path
    quarantine_path: Path


def _sql_path(path: Path) -> str:
    """Return a safely escaped absolute path for DuckDB SQL."""
    return path.resolve().as_posix().replace("'", "''")


def _validate_required_columns(
    connection: duckdb.DuckDBPyConnection,
    source_path: Path,
) -> None:
    """Fail clearly when the source schema is incompatible."""
    rows = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [str(source_path.resolve())],
    ).fetchall()
    observed_columns = {
        str(row[0])
        for row in rows
    }
    missing_columns = sorted(
        REQUIRED_COLUMNS - observed_columns
    )

    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(
            f"Source schema is missing required columns: {missing}"
        )


def _atomic_parquet_copy(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    destination: Path,
) -> None:
    """Write Parquet through a temporary file before promotion."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial_path = destination.with_suffix(
        destination.suffix + ".partial"
    )
    partial_path.unlink(missing_ok=True)

    connection.execute(
        f"""
        COPY (
            {query}
        )
        TO '{_sql_path(partial_path)}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    os.replace(partial_path, destination)


def transform_yellow_partition(
    source_path: Path,
    silver_path: Path,
    quarantine_path: Path,
    year: int,
    month: int,
) -> TransformationResult:
    """Transform one bronze Yellow Taxi partition."""
    month_start = f"{year}-{month:02d}-01"

    with duckdb.connect() as connection:
        _validate_required_columns(
            connection,
            source_path,
        )

        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW classified_trips AS
            WITH normalized AS (
                SELECT
                    CAST(VendorID AS INTEGER) AS vendor_id,
                    CAST(
                        tpep_pickup_datetime AS TIMESTAMP
                    ) AS pickup_at,
                    CAST(
                        tpep_dropoff_datetime AS TIMESTAMP
                    ) AS dropoff_at,
                    CAST(
                        passenger_count AS BIGINT
                    ) AS passenger_count,
                    CAST(
                        trip_distance AS DOUBLE
                    ) AS trip_distance,
                    CAST(RatecodeID AS BIGINT) AS rate_code_id,
                    CAST(
                        store_and_fwd_flag AS VARCHAR
                    ) AS store_and_fwd_flag,
                    CAST(
                        PULocationID AS INTEGER
                    ) AS pickup_location_id,
                    CAST(
                        DOLocationID AS INTEGER
                    ) AS dropoff_location_id,
                    CAST(
                        payment_type AS BIGINT
                    ) AS payment_type,
                    CAST(fare_amount AS DOUBLE) AS fare_amount,
                    CAST(extra AS DOUBLE) AS extra_amount,
                    CAST(mta_tax AS DOUBLE) AS mta_tax,
                    CAST(tip_amount AS DOUBLE) AS tip_amount,
                    CAST(tolls_amount AS DOUBLE) AS tolls_amount,
                    CAST(
                        improvement_surcharge AS DOUBLE
                    ) AS improvement_surcharge,
                    CAST(total_amount AS DOUBLE) AS total_amount,
                    CAST(
                        congestion_surcharge AS DOUBLE
                    ) AS congestion_surcharge,
                    CAST(Airport_fee AS DOUBLE) AS airport_fee
                FROM read_parquet('{_sql_path(source_path)}')
            ),
            keyed AS (
                SELECT
                    *,
                    md5(
                        concat_ws(
                            '|',
                            coalesce(
                                CAST(vendor_id AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(pickup_at AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(dropoff_at AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(passenger_count AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(trip_distance AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(rate_code_id AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                store_and_fwd_flag,
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(pickup_location_id AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(dropoff_location_id AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(payment_type AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(fare_amount AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(extra_amount AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(mta_tax AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(tip_amount AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(tolls_amount AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(
                                    improvement_surcharge AS VARCHAR
                                ),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(total_amount AS VARCHAR),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(
                                    congestion_surcharge AS VARCHAR
                                ),
                                '<NULL>'
                            ),
                            coalesce(
                                CAST(airport_fee AS VARCHAR),
                                '<NULL>'
                            )
                        )
                    ) AS trip_id
                FROM normalized
            ),
            ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY trip_id
                        ORDER BY pickup_at, dropoff_at
                    ) AS duplicate_rank
                FROM keyed
            )
            SELECT
                *,
                CASE
                    WHEN duplicate_rank > 1
                        THEN 'exact_duplicate'
                    WHEN pickup_at IS NULL
                      OR dropoff_at IS NULL
                        THEN 'missing_timestamp'
                    WHEN pickup_at < DATE '{month_start}'
                      OR pickup_at
                        >= DATE '{month_start}'
                           + INTERVAL '1 month'
                        THEN 'pickup_outside_expected_month'
                    WHEN dropoff_at <= pickup_at
                        THEN 'zero_or_negative_duration'
                    WHEN dropoff_at - pickup_at
                        > INTERVAL '24 hours'
                        THEN 'duration_over_24_hours'
                    WHEN trip_distance < 0
                        THEN 'negative_distance'
                    WHEN pickup_location_id
                        NOT BETWEEN 1 AND 265
                        THEN 'invalid_pickup_zone'
                    WHEN dropoff_location_id
                        NOT BETWEEN 1 AND 265
                        THEN 'invalid_dropoff_zone'
                    ELSE NULL
                END AS rejection_reason
            FROM ranked
            """
        )

        silver_query = f"""
            SELECT
                trip_id,
                vendor_id,
                pickup_at,
                dropoff_at,
                date_diff(
                    'minute',
                    pickup_at,
                    dropoff_at
                ) AS duration_minutes,
                passenger_count,
                trip_distance,
                rate_code_id,
                store_and_fwd_flag,
                pickup_location_id,
                dropoff_location_id,
                payment_type,
                fare_amount,
                extra_amount,
                mta_tax,
                tip_amount,
                tolls_amount,
                improvement_surcharge,
                total_amount,
                congestion_surcharge,
                airport_fee,
                passenger_count IS NULL
                    AS passenger_count_missing,
                trip_distance = 0
                    AS zero_distance,
                total_amount < 0
                    AS negative_total_amount,
                {year} AS source_year,
                {month} AS source_month
            FROM classified_trips
            WHERE rejection_reason IS NULL
        """

        quarantine_query = f"""
            SELECT
                *,
                {year} AS source_year,
                {month} AS source_month
            FROM classified_trips
            WHERE rejection_reason IS NOT NULL
        """

        _atomic_parquet_copy(
            connection,
            silver_query,
            silver_path,
        )
        _atomic_parquet_copy(
            connection,
            quarantine_query,
            quarantine_path,
        )

        source_rows = connection.execute(
            "SELECT COUNT(*) FROM classified_trips"
        ).fetchone()[0]
        accepted_rows = connection.execute(
            "SELECT COUNT(*) FROM classified_trips "
            "WHERE rejection_reason IS NULL"
        ).fetchone()[0]
        quarantined_rows = connection.execute(
            "SELECT COUNT(*) FROM classified_trips "
            "WHERE rejection_reason IS NOT NULL"
        ).fetchone()[0]
        duplicate_rows = connection.execute(
            "SELECT COUNT(*) FROM classified_trips "
            "WHERE duplicate_rank > 1"
        ).fetchone()[0]

    if accepted_rows + quarantined_rows != source_rows:
        raise RuntimeError(
            "Transformation row reconciliation failed"
        )

    return TransformationResult(
        source_rows=source_rows,
        accepted_rows=accepted_rows,
        quarantined_rows=quarantined_rows,
        duplicate_rows=duplicate_rows,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )