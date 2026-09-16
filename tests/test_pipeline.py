from pathlib import Path

import duckdb
import pytest

from taxi_pipeline.config import (
    DatasetPartition,
    PipelinePaths,
)
from taxi_pipeline.ingestion import DownloadResult
from taxi_pipeline.pipeline import run_pipeline
from taxi_pipeline.quality import QualityReport


def _partition() -> DatasetPartition:
    return DatasetPartition(
        taxi_type="yellow",
        year=2024,
        month=1,
        url="https://example.com/source.parquet",
    )


def test_run_pipeline_completes_and_records_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = PipelinePaths(tmp_path / "data")

    def fake_download(
        url: str,
        destination: Path,
    ) -> DownloadResult:
        assert url == "https://example.com/source.parquet"

        return DownloadResult(
            path=destination,
            sha256="test-sha256",
            size_bytes=1000,
            row_count=100,
            reused_existing=False,
        )

    def fake_transform(**_kwargs: object) -> None:
        return None

    def fake_quality(**_kwargs: object) -> QualityReport:
        return QualityReport(
            source_rows=100,
            silver_rows=98,
            quarantine_rows=2,
            reconciled=True,
            duplicate_trip_ids=0,
            invalid_silver_rows=0,
            missing_passenger_flags=5,
            zero_distance_flags=3,
            negative_total_flags=1,
            rejection_counts={
                "zero_or_negative_duration": 2
            },
            passed=True,
        )

    monkeypatch.setattr(
        "taxi_pipeline.pipeline.download_parquet",
        fake_download,
    )
    monkeypatch.setattr(
        "taxi_pipeline.pipeline.transform_yellow_partition",
        fake_transform,
    )
    monkeypatch.setattr(
        "taxi_pipeline.pipeline.validate_partition_outputs",
        fake_quality,
    )

    result = run_pipeline(
        partition=_partition(),
        paths=paths,
    )

    assert result.partition_key == "yellow/2024/01"
    assert result.source_rows == 100
    assert result.silver_rows == 98
    assert result.quarantine_rows == 2
    assert result.quality_passed is True

    with duckdb.connect(
        str(paths.audit_db),
        read_only=True,
    ) as connection:
        audit_row = connection.execute(
            """
            SELECT
                status,
                source_sha256,
                source_rows,
                silver_rows,
                quarantine_rows,
                quality_passed
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            [result.run_id],
        ).fetchone()

    assert audit_row == (
        "completed",
        "test-sha256",
        100,
        98,
        2,
        True,
    )


def test_run_pipeline_records_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = PipelinePaths(tmp_path / "data")

    def fail_download(
        url: str,
        destination: Path,
    ) -> DownloadResult:
        raise RuntimeError(
            f"simulated failure for {url} at {destination}"
        )

    monkeypatch.setattr(
        "taxi_pipeline.pipeline.download_parquet",
        fail_download,
    )

    with pytest.raises(
        RuntimeError,
        match="simulated failure",
    ):
        run_pipeline(
            partition=_partition(),
            paths=paths,
        )

    with duckdb.connect(
        str(paths.audit_db),
        read_only=True,
    ) as connection:
        audit_row = connection.execute(
            """
            SELECT
                status,
                error_message
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

    assert audit_row is not None
    assert audit_row[0] == "failed"
    assert "simulated failure" in audit_row[1]