# NYC Taxi Incremental Data Pipeline

A production-style data engineering project that processes official New York City Taxi and Limousine Commission trip data into reliable, analytics-ready datasets.

## Business Problem

The NYC transportation analytics team receives millions of taxi-trip records every month. Analysts need reliable information about trip demand, revenue, travel patterns and high-traffic pickup locations.

Processing every historical file again whenever a new month arrives would waste time and computing resources. The team needs an incremental pipeline that processes new data while preserving previously completed months.

## Project Goal

Build an end-to-end pipeline that:

- Downloads official monthly taxi-trip data.
- Preserves the original source files.
- Validates source schemas and file integrity.
- Cleans and standardizes trip records.
- Separates invalid records for investigation.
- Prevents duplicate processing.
- Creates analytics-ready business tables.
- Records pipeline runs and data-quality results.
- Supports safe monthly incremental loads and historical backfills.

## Planned Data Layers

- **Bronze:** Original source files preserved without business transformations.
- **Silver:** Cleaned, standardized and deduplicated trip records.
- **Quarantine:** Rejected records with documented rejection reasons.
- **Gold:** Aggregated datasets for reporting and business analysis.

## Technology Plan

The project will be developed locally before being deployed to AWS.

- Python
- SQL
- Parquet
- DuckDB
- dbt
- Apache Airflow
- Docker
- GitHub Actions
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- Terraform

Technologies will only be listed as completed after they have been implemented and validated.

## Data Source

NYC Taxi and Limousine Commission Trip Record Data:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Current Status

Project foundation and local development environment setup are in progress.