from __future__ import annotations

import hashlib
import os
from pathlib import Path
from shutil import copyfileobj
from typing import Any
from urllib.request import Request, urlopen

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SUPPORTED_TAXI_TYPES = {"yellow"}


def parse_partition(event: dict[str, Any]) -> tuple[str, int, int]:
    """Validate and normalize one monthly ingestion request."""
    taxi_type = str(event.get("taxi_type", "")).strip().lower()

    if taxi_type not in SUPPORTED_TAXI_TYPES:
        raise ValueError(
            f"Unsupported taxi_type: {taxi_type!r}. "
            f"Supported values: {sorted(SUPPORTED_TAXI_TYPES)}"
        )

    year_value = event.get("year")
    month_value = event.get("month")

    if isinstance(year_value, bool) or isinstance(month_value, bool):
        raise ValueError("year and month must be integers")

    try:
        year = int(year_value)
        month = int(month_value)
    except (TypeError, ValueError) as error:
        raise ValueError("year and month must be integers") from error

    if not 2009 <= year <= 2100:
        raise ValueError("year must be between 2009 and 2100")

    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")

    return taxi_type, year, month


def source_filename(taxi_type: str, year: int, month: int) -> str:
    """Return the official TLC monthly Parquet filename."""
    return f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def source_url(taxi_type: str, year: int, month: int) -> str:
    """Return the trusted TLC download URL for one partition."""
    filename = source_filename(taxi_type, year, month)
    return f"{TLC_BASE_URL}/{filename}"


def bronze_object_key(taxi_type: str, year: int, month: int) -> str:
    """Return the partitioned S3 Bronze object key."""
    filename = source_filename(taxi_type, year, month)

    return (
        f"bronze/taxi_type={taxi_type}/"
        f"year={year}/month={month:02d}/{filename}"
    )


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Calculate the SHA-256 fingerprint of a local file."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            digest.update(chunk)

    return digest.hexdigest()


def validate_parquet_file(path: Path) -> int:
    """Validate basic Parquet magic bytes and return file size."""
    size_bytes = path.stat().st_size

    if size_bytes < 8:
        raise ValueError(f"Downloaded file is too small to be Parquet: {path}")

    with path.open("rb") as file:
        leading_magic = file.read(4)
        file.seek(-4, os.SEEK_END)
        trailing_magic = file.read(4)

    if leading_magic != b"PAR1" or trailing_magic != b"PAR1":
        raise ValueError(f"Downloaded file is not valid Parquet: {path}")

    return size_bytes


def download_source(url: str, destination: Path) -> None:
    """Download one trusted TLC source file to Lambda temporary storage."""
    request = Request(
        url,
        headers={"User-Agent": "nyc-taxi-data-pipeline/0.2"},
    )

    with (
        urlopen(request, timeout=120) as response,
        destination.open("wb") as file,
    ):
        copyfileobj(response, file, length=1024 * 1024)


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Download one TLC partition and store it in the S3 Bronze layer."""
    import boto3

    bucket = os.environ.get("DATA_LAKE_BUCKET")

    if not bucket:
        raise RuntimeError("DATA_LAKE_BUCKET environment variable is required")

    taxi_type, year, month = parse_partition(event)
    filename = source_filename(taxi_type, year, month)
    url = source_url(taxi_type, year, month)
    object_key = bronze_object_key(taxi_type, year, month)

    s3_client = boto3.client("s3")

    try:
        existing = s3_client.head_object(
            Bucket=bucket,
            Key=object_key,
        )

        existing_sha256 = existing.get("Metadata", {}).get("sha256")
        existing_size = int(existing.get("ContentLength", 0))

        if existing_size > 0 and existing_sha256:
            return {
                "taxi_type": taxi_type,
                "year": year,
                "month": month,
                "source_url": url,
                "s3_uri": f"s3://{bucket}/{object_key}",
                "size_bytes": existing_size,
                "sha256": existing_sha256,
                "reused_existing": True,
            }

    except Exception as error:
        error_response = getattr(error, "response", {})
        error_code = str(
            error_response.get("Error", {}).get("Code", "")
        )

        if error_code not in {"404", "NoSuchKey", "NotFound"}:
            raise

    temporary_path = Path("/tmp") / filename

    try:
        download_source(url, temporary_path)
        size_bytes = validate_parquet_file(temporary_path)
        sha256 = sha256_file(temporary_path)

        s3_client.upload_file(
            str(temporary_path),
            bucket,
            object_key,
            ExtraArgs={
                "ContentType": "application/vnd.apache.parquet",
                "Metadata": {
                    "sha256": sha256,
                    "source-url": url,
                },
            },
        )

        uploaded = s3_client.head_object(
            Bucket=bucket,
            Key=object_key,
        )
        uploaded_size = int(uploaded.get("ContentLength", 0))

        if uploaded_size != size_bytes:
            raise RuntimeError(
                "Uploaded S3 object size does not match the downloaded file"
            )

        return {
            "taxi_type": taxi_type,
            "year": year,
            "month": month,
            "source_url": url,
            "s3_uri": f"s3://{bucket}/{object_key}",
            "size_bytes": size_bytes,
            "sha256": sha256,
            "reused_existing": False,
        }

    finally:
        temporary_path.unlink(missing_ok=True)