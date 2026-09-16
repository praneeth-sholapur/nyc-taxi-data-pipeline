from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import httpx

from taxi_pipeline.ingestion import sha256_file


TAXI_ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "misc/taxi_zone_lookup.csv"
)

_REQUIRED_COLUMNS = {
    "LocationID",
    "Borough",
    "Zone",
    "service_zone",
}


@dataclass(frozen=True)
class DimensionBuildResult:
    """Metadata for a completed taxi-zone dimension build."""

    source_path: Path
    output_path: Path
    source_sha256: str
    source_reused: bool
    row_count: int


def _validate_zone_csv(path: Path) -> int:
    """Validate the required taxi-zone lookup fields and values."""
    with duckdb.connect() as connection:
        source = connection.read_csv(
            str(path.resolve()),
            header=True,
            all_varchar=True,
        )
        actual_columns = set(source.columns)
        missing_columns = _REQUIRED_COLUMNS - actual_columns

        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(
                f"Taxi-zone lookup is missing columns: {missing}"
            )

        source.create_view("taxi_zone_source")

        row = connection.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(
                    DISTINCT TRY_CAST(LocationID AS INTEGER)
                ) AS distinct_location_ids,
                COUNT(*) FILTER (
                    WHERE
                        TRY_CAST(LocationID AS INTEGER) IS NULL
                        OR TRY_CAST(LocationID AS INTEGER) <= 0
                        OR TRIM(COALESCE(Borough, '')) = ''
                        OR TRIM(COALESCE(Zone, '')) = ''
                        OR TRIM(COALESCE(service_zone, '')) = ''
                ) AS invalid_rows
            FROM taxi_zone_source
            """
        ).fetchone()

    row_count = int(row[0])
    distinct_location_ids = int(row[1])
    invalid_rows = int(row[2])

    if row_count <= 0:
        raise ValueError("Taxi-zone lookup contains no rows")

    if distinct_location_ids != row_count:
        raise ValueError(
            "Taxi-zone lookup contains duplicate LocationID values"
        )

    if invalid_rows != 0:
        raise ValueError(
            f"Taxi-zone lookup contains {invalid_rows} invalid rows"
        )

    return row_count


def _download_zone_lookup(
    destination: Path,
    url: str,
    retries: int,
) -> bool:
    """Download the lookup atomically and return its reuse status."""
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists():
        _validate_zone_csv(destination)
        return True

    partial_path = destination.with_suffix(
        destination.suffix + ".partial"
    )
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            with httpx.stream(
                "GET",
                url,
                timeout=120.0,
                follow_redirects=True,
            ) as response:
                response.raise_for_status()

                with partial_path.open("wb") as file:
                    for chunk in response.iter_bytes(
                        chunk_size=1024 * 1024
                    ):
                        file.write(chunk)

            _validate_zone_csv(partial_path)
            os.replace(partial_path, destination)
            return False

        except Exception as error:
            last_error = error
            partial_path.unlink(missing_ok=True)

            if attempt < retries:
                time.sleep(2 ** (attempt - 1))

    raise RuntimeError(
        f"Taxi-zone lookup download failed after "
        f"{retries} attempts: {url}"
    ) from last_error


def build_taxi_zone_dimension(
    data_root: Path,
    url: str = TAXI_ZONE_LOOKUP_URL,
    retries: int = 3,
) -> DimensionBuildResult:
    """Download, validate and build the taxi-zone dimension."""
    source_path = (
        data_root
        / "reference"
        / "taxi_zone_lookup.csv"
    )
    output_path = (
        data_root
        / "gold"
        / "dimensions"
        / "dim_taxi_zone.parquet"
    )

    source_reused = _download_zone_lookup(
        destination=source_path,
        url=url,
        retries=retries,
    )
    source_row_count = _validate_zone_csv(source_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(
        f".{output_path.name}.tmp"
    )
    temporary_path.unlink(missing_ok=True)

    try:
        with duckdb.connect() as connection:
            connection.read_csv(
                str(source_path.resolve()),
                header=True,
                all_varchar=True,
            ).create_view("taxi_zone_source")

            dimension = connection.sql(
                """
                SELECT
                    CAST(LocationID AS INTEGER) AS location_id,
                    TRIM(Borough) AS borough,
                    TRIM(Zone) AS zone,
                    TRIM(service_zone) AS service_zone
                FROM taxi_zone_source
                ORDER BY location_id
                """
            )
            dimension.write_parquet(
                str(temporary_path),
                compression="zstd",
            )

            output_row = connection.execute(
                """
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT location_id)
                        AS distinct_location_ids
                FROM read_parquet(?)
                """,
                [str(temporary_path)],
            ).fetchone()

        output_row_count = int(output_row[0])
        distinct_location_ids = int(output_row[1])

        if output_row_count != source_row_count:
            raise RuntimeError(
                "Taxi-zone dimension row count does not match "
                "the validated source"
            )

        if distinct_location_ids != output_row_count:
            raise RuntimeError(
                "Taxi-zone dimension contains duplicate keys"
            )

        os.replace(temporary_path, output_path)

        return DimensionBuildResult(
            source_path=source_path,
            output_path=output_path,
            source_sha256=sha256_file(source_path),
            source_reused=source_reused,
            row_count=output_row_count,
        )
    finally:
        temporary_path.unlink(missing_ok=True)