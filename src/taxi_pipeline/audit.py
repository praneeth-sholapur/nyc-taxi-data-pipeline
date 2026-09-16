from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def _initialize_tables(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Create audit tables when they do not already exist."""
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR PRIMARY KEY,
            partition_key VARCHAR NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            status VARCHAR NOT NULL,
            source_path VARCHAR,
            source_sha256 VARCHAR,
            source_reused BOOLEAN,
            source_rows BIGINT,
            silver_rows BIGINT,
            quarantine_rows BIGINT,
            quality_passed BOOLEAN,
            error_message VARCHAR
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS quality_results (
            run_id VARCHAR NOT NULL,
            check_name VARCHAR NOT NULL,
            passed BOOLEAN NOT NULL,
            observed_value VARCHAR NOT NULL,
            expected_value VARCHAR NOT NULL,
            PRIMARY KEY (run_id, check_name)
        )
        """
    )


def start_audit_run(
    audit_db: Path,
    partition_key: str,
) -> str:
    """Create a running audit record and return its unique ID."""
    audit_db.parent.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    connection = duckdb.connect(str(audit_db))

    try:
        _initialize_tables(connection)
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                run_id,
                partition_key,
                started_at,
                status
            )
            VALUES (?, ?, ?, 'running')
            """,
            [
                run_id,
                partition_key,
                started_at,
            ],
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()

    return run_id


def complete_audit_run(
    audit_db: Path,
    run_id: str,
    source_path: Path,
    source_sha256: str,
    source_reused: bool,
    source_rows: int,
    silver_rows: int,
    quarantine_rows: int,
    reconciled: bool,
    duplicate_trip_ids: int,
    invalid_silver_rows: int,
    quality_passed: bool,
) -> None:
    """Complete a successful pipeline audit record."""
    completed_at = datetime.now(timezone.utc)
    connection = duckdb.connect(str(audit_db))

    try:
        _initialize_tables(connection)

        connection.execute(
            """
            UPDATE pipeline_runs
            SET
                completed_at = ?,
                status = 'completed',
                source_path = ?,
                source_sha256 = ?,
                source_reused = ?,
                source_rows = ?,
                silver_rows = ?,
                quarantine_rows = ?,
                quality_passed = ?,
                error_message = NULL
            WHERE run_id = ?
            """,
            [
                completed_at,
                str(source_path),
                source_sha256,
                source_reused,
                source_rows,
                silver_rows,
                quarantine_rows,
                quality_passed,
                run_id,
            ],
        )

        quality_rows = [
            (
                run_id,
                "row_reconciliation",
                reconciled,
                str(silver_rows + quarantine_rows),
                str(source_rows),
            ),
            (
                run_id,
                "silver_trip_id_uniqueness",
                duplicate_trip_ids == 0,
                str(duplicate_trip_ids),
                "0",
            ),
            (
                run_id,
                "silver_hard_rule_compliance",
                invalid_silver_rows == 0,
                str(invalid_silver_rows),
                "0",
            ),
        ]

        connection.executemany(
            """
            INSERT INTO quality_results (
                run_id,
                check_name,
                passed,
                observed_value,
                expected_value
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            quality_rows,
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()


def fail_audit_run(
    audit_db: Path,
    run_id: str,
    error_message: str,
) -> None:
    """Record a failed pipeline run."""
    completed_at = datetime.now(timezone.utc)
    connection = duckdb.connect(str(audit_db))

    try:
        _initialize_tables(connection)
        connection.execute(
            """
            UPDATE pipeline_runs
            SET
                completed_at = ?,
                status = 'failed',
                error_message = ?
            WHERE run_id = ?
            """,
            [
                completed_at,
                error_message,
                run_id,
            ],
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()