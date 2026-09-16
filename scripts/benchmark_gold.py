from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any

import duckdb


SILVER_QUERY = """
SELECT
    CAST(EXTRACT(HOUR FROM pickup_at) AS INTEGER) AS pickup_hour,
    pickup_location_id,
    COUNT(*)::BIGINT AS trip_count
FROM read_parquet(?, hive_partitioning=true)
GROUP BY
    pickup_hour,
    pickup_location_id
ORDER BY
    pickup_hour,
    pickup_location_id
"""

GOLD_QUERY = """
SELECT
    pickup_hour,
    pickup_location_id,
    SUM(trip_count)::BIGINT AS trip_count
FROM read_parquet(?, hive_partitioning=true)
GROUP BY
    pickup_hour,
    pickup_location_id
ORDER BY
    pickup_hour,
    pickup_location_id
"""


def execute_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parquet_path: Path,
) -> list[tuple[Any, ...]]:
    """Execute one analytical query and return its rows."""
    return connection.execute(
        query,
        [str(parquet_path)],
    ).fetchall()


def benchmark_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parquet_path: Path,
    runs: int,
) -> tuple[list[tuple[Any, ...]], float]:
    """Return query results and the median warmed execution time."""
    expected_rows = execute_query(
        connection,
        query,
        parquet_path,
    )

    durations: list[float] = []

    for _ in range(runs):
        started_at = time.perf_counter()
        actual_rows = execute_query(
            connection,
            query,
            parquet_path,
        )
        durations.append(time.perf_counter() - started_at)

        if actual_rows != expected_rows:
            raise RuntimeError(
                "Benchmark query returned inconsistent results"
            )

    return expected_rows, statistics.median(durations)


def count_parquet_rows(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: Path,
) -> int:
    """Count physical records across a Parquet path pattern."""
    return connection.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?, hive_partitioning=true)
        """,
        [str(parquet_path)],
    ).fetchone()[0]


def run_benchmark(
    data_root: Path,
    runs: int,
) -> dict[str, Any]:
    """Compare equivalent Silver and Gold analytical queries."""
    if runs < 1:
        raise ValueError("runs must be at least 1")

    silver_path = (
        data_root
        / "silver"
        / "taxi_type=yellow"
        / "year=2024"
        / "month=*"
        / "trips.parquet"
    )
    gold_path = (
        data_root
        / "gold"
        / "pickup_zone_hourly"
        / "taxi_type=yellow"
        / "year=2024"
        / "month=*"
        / "metrics.parquet"
    )

    with duckdb.connect() as connection:
        silver_input_rows = count_parquet_rows(
            connection,
            silver_path,
        )
        gold_input_rows = count_parquet_rows(
            connection,
            gold_path,
        )

        silver_results, silver_seconds = benchmark_query(
            connection,
            SILVER_QUERY,
            silver_path,
            runs,
        )
        gold_results, gold_seconds = benchmark_query(
            connection,
            GOLD_QUERY,
            gold_path,
            runs,
        )

    if silver_results != gold_results:
        raise RuntimeError(
            "Silver and Gold queries produced different results"
        )

    represented_trips = sum(
        row[2]
        for row in gold_results
    )

    return {
        "runs": runs,
        "timing_method": "median_after_one_warmup",
        "result_rows": len(gold_results),
        "represented_trips": represented_trips,
        "silver_input_rows": silver_input_rows,
        "gold_input_rows": gold_input_rows,
        "row_reduction_percent": round(
            (1 - gold_input_rows / silver_input_rows) * 100,
            2,
        ),
        "silver_median_seconds": round(silver_seconds, 6),
        "gold_median_seconds": round(gold_seconds, 6),
        "speedup": round(
            silver_seconds / gold_seconds,
            2,
        ),
    }


def main() -> None:
    """Run the command-line benchmark."""
    parser = argparse.ArgumentParser(
        description="Benchmark equivalent Silver and Gold queries"
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
    )
    args = parser.parse_args()

    result = run_benchmark(
        data_root=args.data_root,
        runs=args.runs,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()