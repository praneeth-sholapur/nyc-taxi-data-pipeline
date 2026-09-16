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
from taxi_pipeline.ingestion import download_parquet
from taxi_pipeline.profiling import (
    get_parquet_schema,
    profile_yellow_trips,
)


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

    return parser


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

    partition_directory = (
        Path(f"taxi_type={partition.taxi_type}")
        / f"year={partition.year}"
        / f"month={partition.month:02d}"
    )
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

    output = {
        "path": str(bronze_path),
        "schema": schema,
        "profile": profile,
    }

    print(json.dumps(output, indent=2, default=str))


def main() -> None:
    """Run the requested pipeline command."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "ingest":
        ingest_partition(args)
    elif args.command == "profile":
        profile_partition(args)


if __name__ == "__main__":
    main()