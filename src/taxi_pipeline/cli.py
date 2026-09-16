from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from taxi_pipeline.config import (
    PipelinePaths,
    load_manifest,
    select_partition,
)
from taxi_pipeline.ingestion import download_parquet


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
    ingest.add_argument(
        "--manifest",
        type=Path,
        default=Path("config/manifest.json"),
    )
    ingest.add_argument(
        "--year",
        type=int,
        required=True,
    )
    ingest.add_argument(
        "--month",
        type=int,
        choices=range(1, 13),
        required=True,
    )
    ingest.add_argument(
        "--taxi-type",
        default="yellow",
    )
    ingest.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
    )

    return parser


def ingest_partition(args: argparse.Namespace) -> None:
    """Ingest one selected monthly partition."""
    manifest = load_manifest(args.manifest)
    partition = select_partition(
        manifest,
        year=args.year,
        month=args.month,
        taxi_type=args.taxi_type,
    )

    paths = PipelinePaths(args.data_root)
    paths.ensure()

    partition_directory = (
        Path(f"taxi_type={partition.taxi_type}")
        / f"year={partition.year}"
        / f"month={partition.month:02d}"
    )
    destination = (
        paths.bronze
        / partition_directory
        / partition.filename
    )

    result = download_parquet(
        url=partition.url,
        destination=destination,
    )

    output = asdict(result)
    output["path"] = str(result.path)

    print(json.dumps(output, indent=2))


def main() -> None:
    """Run the requested pipeline command."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "ingest":
        ingest_partition(args)


if __name__ == "__main__":
    main()