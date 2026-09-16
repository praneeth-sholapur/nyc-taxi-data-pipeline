from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from taxi_pipeline.config import (
    DatasetPartition,
    PipelinePaths,
    load_manifest,
    select_partition,
)
from taxi_pipeline.dimensions import (
    TAXI_ZONE_LOOKUP_URL,
    build_taxi_zone_dimension,
)
from taxi_pipeline.gold import build_gold_partition
from taxi_pipeline.ingestion import download_parquet
from taxi_pipeline.pipeline import run_pipeline
from taxi_pipeline.profiling import (
    count_exact_duplicate_rows,
    get_parquet_schema,
    profile_yellow_trips,
)
from taxi_pipeline.quality import validate_partition_outputs
from taxi_pipeline.transformation import transform_yellow_partition


def add_partition_arguments(
    command: argparse.ArgumentParser,
) -> None:
    """Add shared monthly-partition arguments to a command."""
    command.add_argument(
        "--manifest",
        type=Path,
        default=Path("config/manifest.json"),
    )
    command.add_argument(
        "--year",
        type=int,
        required=True,
    )
    command.add_argument(
        "--month",
        type=int,
        choices=range(1, 13),
        required=True,
    )
    command.add_argument(
        "--taxi-type",
        default="yellow",
    )
    command.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
    )


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="NYC TLC incremental data pipeline"
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    ingest = subparsers.add_parser(
        "ingest",
        help="Download one source partition into the bronze layer",
    )
    add_partition_arguments(ingest)

    profile = subparsers.add_parser(
        "profile",
        help="Profile one bronze source partition",
    )
    add_partition_arguments(profile)

    transform = subparsers.add_parser(
        "transform",
        help="Create silver and quarantine outputs",
    )
    add_partition_arguments(transform)

    validate = subparsers.add_parser(
        "validate",
        help="Validate transformed partition outputs",
    )
    add_partition_arguments(validate)

    gold = subparsers.add_parser(
        "gold",
        help="Create business-ready Gold metrics",
    )
    add_partition_arguments(gold)

    dimension = subparsers.add_parser(
        "dimension",
        help="Build the taxi-zone dimension",
    )
    dimension.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
    )
    dimension.add_argument(
        "--url",
        default=TAXI_ZONE_LOOKUP_URL,
    )

    run = subparsers.add_parser(
        "run",
        help="Run the complete audited pipeline",
    )
    add_partition_arguments(run)

    return parser


def get_partition_directory(
    partition: DatasetPartition,
) -> Path:
    """Return the Hive-style directory for one partition."""
    return (
        Path(f"taxi_type={partition.taxi_type}")
        / f"year={partition.year}"
        / f"month={partition.month:02d}"
    )


def resolve_partition(
    args: argparse.Namespace,
) -> tuple[DatasetPartition, PipelinePaths, Path]:
    """Resolve configuration and the expected bronze file path."""
    manifest = load_manifest(args.manifest)
    partition = select_partition(
        manifest,
        year=args.year,
        month=args.month,
        taxi_type=args.taxi_type,
    )
    paths = PipelinePaths(args.data_root)
    partition_directory = get_partition_directory(partition)

    bronze_path = (
        paths.bronze
        / partition_directory
        / partition.filename
    )

    return partition, paths, bronze_path


def ingest_partition(args: argparse.Namespace) -> None:
    """Ingest one selected monthly partition."""
    partition, paths, bronze_path = resolve_partition(args)
    paths.ensure()

    result = download_parquet(
        url=partition.url,
        destination=bronze_path,
    )

    output = asdict(result)
    output["path"] = str(result.path)

    print(json.dumps(output, indent=2))


def profile_partition(args: argparse.Namespace) -> None:
    """Profile one previously ingested bronze partition."""
    partition, _paths, bronze_path = resolve_partition(args)

    if not bronze_path.exists():
        raise FileNotFoundError(
            f"Bronze source does not exist: {bronze_path}. "
            "Run the ingest command first."
        )

    schema = [
        {
            "column_name": column_name,
            "data_type": data_type,
        }
        for column_name, data_type in get_parquet_schema(bronze_path)
    ]

    profile = profile_yellow_trips(
        bronze_path,
        year=partition.year,
        month=partition.month,
    )
    profile["exact_duplicate_rows"] = (
        count_exact_duplicate_rows(bronze_path)
    )

    output = {
        "path": str(bronze_path),
        "schema": schema,
        "profile": profile,
    }

    print(json.dumps(output, indent=2, default=str))


def transform_partition(args: argparse.Namespace) -> None:
    """Create silver and quarantine outputs for one partition."""
    partition, paths, bronze_path = resolve_partition(args)

    if not bronze_path.exists():
        raise FileNotFoundError(
            f"Bronze source does not exist: {bronze_path}. "
            "Run the ingest command first."
        )

    paths.ensure()
    partition_directory = get_partition_directory(partition)

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

    result = transform_yellow_partition(
        source_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
        year=partition.year,
        month=partition.month,
    )

    output = asdict(result)
    output["silver_path"] = str(result.silver_path)
    output["quarantine_path"] = str(
        result.quarantine_path
    )

    print(json.dumps(output, indent=2))


def validate_partition(args: argparse.Namespace) -> None:
    """Validate one transformed partition."""
    partition, paths, bronze_path = resolve_partition(args)
    partition_directory = get_partition_directory(partition)

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

    report = validate_partition_outputs(
        source_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
        year=partition.year,
        month=partition.month,
    )

    print(json.dumps(asdict(report), indent=2))

    if not report.passed:
        raise SystemExit(1)


def build_gold_metrics(args: argparse.Namespace) -> None:
    """Create Gold metrics for one transformed partition."""
    partition, _paths, _bronze_path = resolve_partition(args)

    result = build_gold_partition(
        data_root=args.data_root,
        taxi_type=partition.taxi_type,
        year=partition.year,
        month=partition.month,
    )

    output = asdict(result)
    output["output_path"] = str(result.output_path)

    print(json.dumps(output, indent=2))


def build_zone_dimension(args: argparse.Namespace) -> None:
    """Build the validated taxi-zone dimension."""
    result = build_taxi_zone_dimension(
        data_root=args.data_root,
        url=args.url,
    )

    output = asdict(result)
    output["source_path"] = str(result.source_path)
    output["output_path"] = str(result.output_path)

    print(json.dumps(output, indent=2))


def run_full_pipeline(args: argparse.Namespace) -> None:
    """Run the complete audited pipeline."""
    partition, paths, _bronze_path = resolve_partition(args)

    result = run_pipeline(
        partition=partition,
        paths=paths,
    )

    output = asdict(result)
    output["silver_path"] = str(result.silver_path)
    output["quarantine_path"] = str(
        result.quarantine_path
    )
    output["gold_path"] = str(result.gold_path)

    print(json.dumps(output, indent=2))


def main() -> None:
    """Run the requested pipeline command."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "ingest":
        ingest_partition(args)
    elif args.command == "profile":
        profile_partition(args)
    elif args.command == "transform":
        transform_partition(args)
    elif args.command == "validate":
        validate_partition(args)
    elif args.command == "gold":
        build_gold_metrics(args)
    elif args.command == "dimension":
        build_zone_dimension(args)
    elif args.command == "run":
        run_full_pipeline(args)


if __name__ == "__main__":
    main()