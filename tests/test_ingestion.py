import hashlib
from pathlib import Path

import duckdb
import pytest

from taxi_pipeline.ingestion import (
    download_parquet,
    sha256_file,
    validate_parquet,
)


def _write_parquet(path: Path, select_sql: str) -> None:
    """Create a temporary Parquet file using a controlled SQL query."""
    safe_path = path.resolve().as_posix().replace("'", "''")

    with duckdb.connect() as connection:
        connection.execute(
            f"COPY ({select_sql}) TO '{safe_path}' (FORMAT PARQUET)"
        )


def test_sha256_file_returns_expected_fingerprint(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.txt"
    content = b"nyc taxi data"
    source.write_bytes(content)

    expected = hashlib.sha256(content).hexdigest()

    assert sha256_file(source) == expected


def test_validate_parquet_returns_row_count(tmp_path: Path) -> None:
    parquet_path = tmp_path / "trips.parquet"
    _write_parquet(
        parquet_path,
        """
        SELECT *
        FROM (VALUES (1, 'complete'), (2, 'pending'))
        AS trips(trip_id, status)
        """,
    )

    assert validate_parquet(parquet_path) == 2


def test_validate_parquet_rejects_empty_file(tmp_path: Path) -> None:
    parquet_path = tmp_path / "empty.parquet"
    _write_parquet(
        parquet_path,
        "SELECT 1 AS trip_id WHERE FALSE",
    )

    with pytest.raises(ValueError, match="has no rows"):
        validate_parquet(parquet_path)


def test_download_reuses_existing_valid_file(tmp_path: Path) -> None:
    destination = tmp_path / "existing.parquet"
    _write_parquet(
        destination,
        "SELECT 101 AS trip_id",
    )

    result = download_parquet(
        url="https://example.invalid/not-used.parquet",
        destination=destination,
    )

    assert result.path == destination
    assert result.row_count == 1
    assert result.size_bytes > 0
    assert result.sha256 == sha256_file(destination)
    assert result.reused_existing is True


def test_download_failure_removes_partial_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "bronze" / "trips.parquet"
    partial_path = destination.with_suffix(".parquet.partial")

    destination.parent.mkdir(parents=True)
    partial_path.write_bytes(b"incomplete data")

    def fail_stream(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated network failure")

    monkeypatch.setattr(
        "taxi_pipeline.ingestion.httpx.stream",
        fail_stream,
    )

    with pytest.raises(RuntimeError, match="after 1 attempts"):
        download_parquet(
            url="https://example.invalid/trips.parquet",
            destination=destination,
            retries=1,
        )

    assert not partial_path.exists()
    assert not destination.exists()