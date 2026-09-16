from pathlib import Path

from taxi_pipeline.cli import build_parser


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