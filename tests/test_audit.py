from pathlib import Path

import duckdb

from taxi_pipeline.audit import (
    complete_audit_run,
    fail_audit_run,
    start_audit_run,
)


def test_successful_audit_run_records_metrics(
    tmp_path: Path,
) -> None:
    audit_db = tmp_path / "audit.duckdb"
    gold_path = Path("data/gold/metrics.parquet")

    run_id = start_audit_run(
        audit_db=audit_db,
        partition_key="yellow/2024/01",
    )

    complete_audit_run(
        audit_db=audit_db,
        run_id=run_id,
        source_path=Path("data/bronze/source.parquet"),
        source_sha256="abc123",
        source_reused=False,
        source_rows=100,
        silver_rows=98,
        quarantine_rows=2,
        reconciled=True,
        duplicate_trip_ids=0,
        invalid_silver_rows=0,
        quality_passed=True,
        gold_path=gold_path,
        gold_rows=20,
        gold_trip_count=98,
        gold_reconciled=True,
    )

    with duckdb.connect(
        str(audit_db),
        read_only=True,
    ) as connection:
        run = connection.execute(
            """
            SELECT
                partition_key,
                status,
                source_sha256,
                source_reused,
                source_rows,
                silver_rows,
                quarantine_rows,
                gold_path,
                gold_rows,
                gold_trip_count,
                quality_passed,
                error_message
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()

        quality_rows = connection.execute(
            """
            SELECT
                check_name,
                passed,
                observed_value,
                expected_value
            FROM quality_results
            WHERE run_id = ?
            ORDER BY check_name
            """,
            [run_id],
        ).fetchall()

    assert run == (
        "yellow/2024/01",
        "completed",
        "abc123",
        False,
        100,
        98,
        2,
        str(gold_path),
        20,
        98,
        True,
        None,
    )

    assert quality_rows == [
        (
            "gold_trip_reconciliation",
            True,
            "98",
            "98",
        ),
        (
            "row_reconciliation",
            True,
            "100",
            "100",
        ),
        (
            "silver_hard_rule_compliance",
            True,
            "0",
            "0",
        ),
        (
            "silver_trip_id_uniqueness",
            True,
            "0",
            "0",
        ),
    ]


def test_failed_audit_run_records_error(
    tmp_path: Path,
) -> None:
    audit_db = tmp_path / "audit.duckdb"

    run_id = start_audit_run(
        audit_db=audit_db,
        partition_key="yellow/2024/02",
    )

    fail_audit_run(
        audit_db=audit_db,
        run_id=run_id,
        error_message="simulated pipeline failure",
    )

    with duckdb.connect(
        str(audit_db),
        read_only=True,
    ) as connection:
        run = connection.execute(
            """
            SELECT
                status,
                error_message
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()

    assert run == (
        "failed",
        "simulated pipeline failure",
    )