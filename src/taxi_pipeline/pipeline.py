from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from taxi_pipeline.audit import (
    complete_audit_run,
    fail_audit_run,
    start_audit_run,
)
from taxi_pipeline.config import (
    DatasetPartition,
    PipelinePaths,
)
from taxi_pipeline.gold import build_gold_partition
from taxi_pipeline.ingestion import download_parquet
from taxi_pipeline.quality import validate_partition_outputs
from taxi_pipeline.transformation import transform_yellow_partition


@dataclass(frozen=True)
class PipelineRunResult:
    """Summary of one completed end-to-end pipeline run."""

    run_id: str
    partition_key: str
    source_reused: bool
    source_rows: int
    silver_rows: int
    quarantine_rows: int
    gold_rows: int
    gold_trip_count: int
    quality_passed: bool
    silver_path: Path
    quarantine_path: Path
    gold_path: Path


def _partition_directory(
    partition: DatasetPartition,
) -> Path:
    """Return the Hive-style directory for one partition."""
    return (
        Path(f"taxi_type={partition.taxi_type}")
        / f"year={partition.year}"
        / f"month={partition.month:02d}"
    )


def run_pipeline(
    partition: DatasetPartition,
    paths: PipelinePaths,
) -> PipelineRunResult:
    """Run ingestion, transformation, validation and Gold aggregation."""
    paths.ensure()
    partition_directory = _partition_directory(partition)

    bronze_path = (
        paths.bronze
        / partition_directory
        / partition.filename
    )
    silver_path = (
        paths.silver
        / partition_directory
        / "trips.parquet"
    )
    quarantine_path = (
        paths.quarantine
        / partition_directory
        / "rejected.parquet"
    )

    run_id = start_audit_run(
        audit_db=paths.audit_db,
        partition_key=partition.key,
    )

    try:
        download = download_parquet(
            url=partition.url,
            destination=bronze_path,
        )

        transform_yellow_partition(
            source_path=bronze_path,
            silver_path=silver_path,
            quarantine_path=quarantine_path,
            year=partition.year,
            month=partition.month,
        )

        quality = validate_partition_outputs(
            source_path=bronze_path,
            silver_path=silver_path,
            quarantine_path=quarantine_path,
            year=partition.year,
            month=partition.month,
        )

        if not quality.passed:
            raise RuntimeError(
                f"Quality validation failed for "
                f"{partition.key}"
            )

        gold = build_gold_partition(
            data_root=paths.silver.parent,
            taxi_type=partition.taxi_type,
            year=partition.year,
            month=partition.month,
        )
        gold_reconciled = (
            gold.total_trip_count == quality.silver_rows
        )

        if not gold_reconciled:
            raise RuntimeError(
                f"Gold reconciliation failed for "
                f"{partition.key}: "
                f"{gold.total_trip_count} != "
                f"{quality.silver_rows}"
            )

        complete_audit_run(
            audit_db=paths.audit_db,
            run_id=run_id,
            source_path=bronze_path,
            source_sha256=download.sha256,
            source_reused=download.reused_existing,
            source_rows=quality.source_rows,
            silver_rows=quality.silver_rows,
            quarantine_rows=quality.quarantine_rows,
            reconciled=quality.reconciled,
            duplicate_trip_ids=quality.duplicate_trip_ids,
            invalid_silver_rows=quality.invalid_silver_rows,
            quality_passed=quality.passed and gold_reconciled,
            gold_path=gold.output_path,
            gold_rows=gold.gold_rows,
            gold_trip_count=gold.total_trip_count,
            gold_reconciled=gold_reconciled,
        )

        return PipelineRunResult(
            run_id=run_id,
            partition_key=partition.key,
            source_reused=download.reused_existing,
            source_rows=quality.source_rows,
            silver_rows=quality.silver_rows,
            quarantine_rows=quality.quarantine_rows,
            gold_rows=gold.gold_rows,
            gold_trip_count=gold.total_trip_count,
            quality_passed=quality.passed and gold_reconciled,
            silver_path=silver_path,
            quarantine_path=quarantine_path,
            gold_path=gold.output_path,
        )

    except Exception as error:
        fail_audit_run(
            audit_db=paths.audit_db,
            run_id=run_id,
            error_message=str(error),
        )
        raise