import json
from pathlib import Path

import pytest

from taxi_pipeline.config import DatasetPartition,PipelinePaths, load_manifest, select_partition


def test_load_and_select_partition(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_data = {
        "datasets": [
            {
                "taxi_type": "yellow",
                "year": 2024,
                "month": 1,
                "url": "https://example.com/yellow-2024-01.parquet",
            }
        ]
    }
    manifest_path.write_text(json.dumps(manifest_data), encoding="utf-8")

    manifest = load_manifest(manifest_path)
    partition = select_partition(manifest, year=2024, month=1)

    assert partition.key == "yellow/2024/01"
    assert partition.filename == "yellow_tripdata_2024-01.parquet"
    assert partition.url == "https://example.com/yellow-2024-01.parquet"


def test_select_partition_rejects_missing_month() -> None:
    manifest = [
        DatasetPartition(
            taxi_type="yellow",
            year=2024,
            month=1,
            url="https://example.com/yellow-2024-01.parquet",
        )
    ]

    with pytest.raises(ValueError, match="found 0"):
        select_partition(manifest, year=2024, month=2)


def test_pipeline_paths_create_required_directories(
    tmp_path: Path,
) -> None:
    paths = PipelinePaths(tmp_path / "data")

    paths.ensure()

    assert paths.bronze.is_dir()
    assert paths.silver.is_dir()
    assert paths.quarantine.is_dir()
    assert paths.gold.is_dir()
    assert paths.audit_db == tmp_path / "data" / "audit.duckdb"        