# NYC Taxi Incremental Data Pipeline v0.1.0

Version 0.1.0 is the first verified portfolio release of the NYC Taxi Incremental Data Pipeline.

## Highlights

- Processes official NYC TLC Yellow Taxi data for January through March 2024
- Handles 9,554,778 Bronze source records
- Produces 9,551,872 validated Silver records
- Preserves 2,906 rejected records in Quarantine
- Produces 240,850 hourly pickup-zone Gold metrics
- Represents all 9,551,872 accepted trips in Gold
- Builds a validated 265-row taxi-zone dimension
- Achieves 100% taxi-zone join coverage
- Records pipeline runs and quality results in DuckDB
- Supports safe, repeatable monthly execution
- Includes reproducible local and containerized workflows

## Data Quality

Automated controls verify:

- Bronze-to-Silver-and-Quarantine row reconciliation
- Silver trip-ID uniqueness
- Silver hard-rule compliance
- Gold-to-Silver trip reconciliation
- Gold analytical-grain uniqueness
- Positive Gold trip counts
- Taxi-zone dimension schema and key integrity

## Performance

The verified benchmark compares equivalent business queries across the Silver and Gold layers.

- Silver input rows: 9,551,872
- Gold input rows: 240,850
- Input-row reduction: 97.48%
- Silver median query time: 0.042986 seconds
- Gold median query time: 0.008073 seconds
- Measured query speedup: 5.32 times

The benchmark uses one warm-up followed by the median of 15 complete executions.

## Containerization

The multi-stage Docker build includes:

- A reusable Python 3.12 base
- A test stage that runs Ruff and Pytest
- A smaller runtime stage
- Non-root runtime user ID 10001
- Persistent `/app/data` storage
- Verified Linux ARM64 execution
- Verified Linux AMD64 execution through GitHub Actions

## Continuous Integration

GitHub Actions automatically runs:

1. Python linting and all automated tests
2. Docker test-image construction
3. Docker runtime-image construction
4. Runtime CLI verification
5. Non-root runtime verification

Current automated test result:

```text
27 passed