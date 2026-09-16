from pathlib import Path

from taxi_pipeline.cli import build_parser
from taxi_pipeline.dimensions import TAXI_ZONE_LOOKUP_URL


def test_ingest_command_parses_required_arguments() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "ingest",
            "--year",
            "2024",
            "--month",
            "1",
        ]
    )

    assert args.command == "ingest"
    assert args.year == 2024
    assert args.month == 1
    assert args.taxi_type == "yellow"
    assert args.manifest == Path("config/manifest.json")
    assert args.data_root == Path("data")


def test_dimension_command_uses_expected_defaults() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "dimension",
        ]
    )

    assert args.command == "dimension"
    assert args.data_root == Path("data")
    assert args.url == TAXI_ZONE_LOOKUP_URL