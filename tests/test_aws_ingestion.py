from pathlib import Path

import pytest

from taxi_pipeline.aws_ingestion import (
    bronze_object_key,
    parse_partition,
    sha256_file,
    source_url,
    validate_parquet_file,
)


def test_parse_partition_normalizes_valid_request() -> None:
    result = parse_partition(
        {
            "taxi_type": " Yellow ",
            "year": "2024",
            "month": "3",
        }
    )

    assert result == ("yellow", 2024, 3)


@pytest.mark.parametrize(
    ("event", "message"),
    [
        (
            {
                "taxi_type": "green",
                "year": 2024,
                "month": 3,
            },
            "Unsupported taxi_type",
        ),
        (
            {
                "taxi_type": "yellow",
                "year": 2008,
                "month": 3,
            },
            "year must be between",
        ),
        (
            {
                "taxi_type": "yellow",
                "year": 2024,
                "month": 13,
            },
            "month must be between",
        ),
        (
            {
                "taxi_type": "yellow",
                "year": True,
                "month": 3,
            },
            "must be integers",
        ),
    ],
)
def test_parse_partition_rejects_invalid_request(
    event: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        parse_partition(event)


def test_source_url_uses_trusted_tlc_location() -> None:
    result = source_url("yellow", 2024, 3)

    assert result == (
        "https://d37ci6vzurychx.cloudfront.net/"
        "trip-data/yellow_tripdata_2024-03.parquet"
    )


def test_bronze_object_key_uses_partitioned_layout() -> None:
    result = bronze_object_key("yellow", 2024, 3)

    assert result == (
        "bronze/taxi_type=yellow/year=2024/month=03/"
        "yellow_tripdata_2024-03.parquet"
    )


def test_validate_parquet_file_accepts_magic_bytes(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "valid.parquet"
    parquet_path.write_bytes(b"PAR1example-payloadPAR1")

    size_bytes = validate_parquet_file(parquet_path)

    assert size_bytes == parquet_path.stat().st_size


@pytest.mark.parametrize(
    "content",
    [
        b"BAD!example-payloadPAR1",
        b"PAR1example-payloadBAD!",
        b"PAR1",
    ],
)
def test_validate_parquet_file_rejects_invalid_content(
    tmp_path: Path,
    content: bytes,
) -> None:
    parquet_path = tmp_path / "invalid.parquet"
    parquet_path.write_bytes(content)

    with pytest.raises(ValueError, match="Parquet"):
        validate_parquet_file(parquet_path)


def test_sha256_file_returns_expected_fingerprint(
    tmp_path: Path,
) -> None:
    file_path = tmp_path / "source.bin"
    file_path.write_bytes(b"abc")

    assert sha256_file(file_path) == (
        "ba7816bf8f01cfea414140de5dae2223"
        "b00361a396177a9cb410ff61f20015ad"
    )