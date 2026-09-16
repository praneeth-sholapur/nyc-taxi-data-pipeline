from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DatasetPartition:
    """One monthly source-data partition."""

    taxi_type: str
    year: int
    month: int
    url: str

    @property
    def key(self) -> str:
        """Return a stable identifier for this partition."""
        return f"{self.taxi_type}/{self.year}/{self.month:02d}"

    @property
    def filename(self) -> str:
        """Return the expected source filename."""
        return f"{self.taxi_type}_tripdata_{self.year}-{self.month:02d}.parquet"

@dataclass(frozen=True)
class PipelinePaths:
    """Filesystem locations used by the local pipeline."""

    root: Path

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def audit_db(self) -> Path:
        return self.root / "audit.duckdb"

    def ensure(self) -> None:
        """Create the required data-layer directories."""
        for path in (
            self.bronze,
            self.silver,
            self.quarantine,
            self.gold,
        ):
            path.mkdir(parents=True, exist_ok=True)

def load_manifest(path: Path) -> list[DatasetPartition]:
    """Load dataset partitions from a JSON manifest."""
    payload = json.loads(path.read_text(encoding="utf-8"))

    return [
        DatasetPartition(
            taxi_type=item["taxi_type"],
            year=item["year"],
            month=item["month"],
            url=item["url"],
        )
        for item in payload["datasets"]
    ]


def select_partition(
    manifest: list[DatasetPartition],
    year: int,
    month: int,
    taxi_type: str = "yellow",
) -> DatasetPartition:
    """Select exactly one requested partition."""
    matches = [
        partition
        for partition in manifest
        if partition.year == year
        and partition.month == month
        and partition.taxi_type == taxi_type
    ]

    if len(matches) != 1:
        raise ValueError(
            f"Expected one manifest entry for "
            f"{taxi_type} {year}-{month:02d}; found {len(matches)}"
        )

    return matches[0]