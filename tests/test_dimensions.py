from pathlib import Path

import duckdb
import pytest

from taxi_pipeline.dimensions import (
    build_taxi_zone_dimension,
)


def _write_lookup(
    data_root: Path,
    contents: str,
) -> Path:
    source_path = (
        data_root
        / "reference"
        / "taxi_zone_lookup.csv"
    )
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(
        contents,
        encoding="utf-8",
    )
    return source_path


def test_build_taxi_zone_dimension(
    tmp_path: Path,
) -> None:
    source_path = _write_lookup(
        tmp_path,
        (
            "LocationID,Borough,Zone,service_zone\n"
            "1,EWR,Newark Airport,EWR\n"
            "2,Queens,Jamaica Bay,Boro Zone\n"
        ),
    )

    result = build_taxi_zone_dimension(tmp_path)

    assert result.source_path == source_path
    assert result.source_reused is True
    assert result.row_count == 2
    assert len(result.source_sha256) == 64
    assert result.output_path.is_file()

    with duckdb.connect() as connection:
        rows = connection.execute(
            """
            SELECT
                location_id,
                borough,
                zone,
                service_zone
            FROM read_parquet(?)
            ORDER BY location_id
            """,
            [str(result.output_path)],
        ).fetchall()

    assert rows == [
        (
            1,
            "EWR",
            "Newark Airport",
            "EWR",
        ),
        (
            2,
            "Queens",
            "Jamaica Bay",
            "Boro Zone",
        ),
    ]


def test_taxi_zone_dimension_rejects_missing_columns(
    tmp_path: Path,
) -> None:
    _write_lookup(
        tmp_path,
        (
            "LocationID,Borough,Zone\n"
            "1,EWR,Newark Airport\n"
        ),
    )

    with pytest.raises(
        ValueError,
        match="missing columns: service_zone",
    ):
        build_taxi_zone_dimension(tmp_path)


def test_taxi_zone_dimension_rejects_duplicate_keys(
    tmp_path: Path,
) -> None:
    _write_lookup(
        tmp_path,
        (
            "LocationID,Borough,Zone,service_zone\n"
            "1,EWR,Newark Airport,EWR\n"
            "1,Queens,Jamaica Bay,Boro Zone\n"
        ),
    )

    with pytest.raises(
        ValueError,
        match="duplicate LocationID",
    ):
        build_taxi_zone_dimension(tmp_path)