from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import httpx


@dataclass(frozen=True)
class DownloadResult:
    """Metadata captured for one downloaded source file."""

    path: Path
    sha256: str
    size_bytes: int
    row_count: int
    reused_existing: bool


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Calculate the SHA-256 fingerprint of a file."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            digest.update(chunk)

    return digest.hexdigest()


def validate_parquet(path: Path) -> int:
    """Confirm that a Parquet file is readable and contains rows."""
    with duckdb.connect() as connection:
        result = connection.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(path.resolve())],
        ).fetchone()

    row_count = int(result[0]) if result else 0

    if row_count <= 0:
        raise ValueError(f"Parquet file has no rows: {path}")

    return row_count


def download_parquet(
    url: str,
    destination: Path,
    retries: int = 3,
) -> DownloadResult:
    """Download and validate one immutable Parquet source file."""
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists():
        return DownloadResult(
            path=destination,
            sha256=sha256_file(destination),
            size_bytes=destination.stat().st_size,
            row_count=validate_parquet(destination),
            reused_existing=True,
        )

    partial_path = destination.with_suffix(destination.suffix + ".partial")
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            with httpx.stream(
                "GET",
                url,
                timeout=120.0,
                follow_redirects=True,
            ) as response:
                response.raise_for_status()

                with partial_path.open("wb") as file:
                    for chunk in response.iter_bytes(
                        chunk_size=1024 * 1024
                    ):
                        file.write(chunk)

            row_count = validate_parquet(partial_path)
            os.replace(partial_path, destination)

            return DownloadResult(
                path=destination,
                sha256=sha256_file(destination),
                size_bytes=destination.stat().st_size,
                row_count=row_count,
                reused_existing=False,
            )

        except Exception as error:
            last_error = error
            partial_path.unlink(missing_ok=True)

            if attempt < retries:
                time.sleep(2 ** (attempt - 1))

    raise RuntimeError(
        f"Download failed after {retries} attempts: {url}"
    ) from last_error