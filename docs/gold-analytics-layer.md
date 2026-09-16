# Gold Analytics Layer

## Purpose

The Gold layer converts cleaned trip-level Silver data into business-ready hourly pickup-zone metrics.

This layer supports questions such as:

- Which pickup zones have the highest trip demand?
- At what hours does demand peak?
- How much fare, tip, and total revenue is associated with each zone?
- What is the average trip duration and distance?
- How many accepted records still contain soft data-quality flags?

## Data Grain

Each Gold row represents one unique combination of:

- Service date
- Pickup hour
- Pickup location ID

This grain supports daily, hourly, and pickup-zone analysis without scanning every individual trip.

## Gold Metrics

Each Gold record contains:

- Trip count
- Total known passenger count
- Total trip distance
- Total fare amount
- Total tip amount
- Net total amount
- Average trip duration
- Average trip distance
- Average total amount
- Missing-passenger flag count
- Zero-distance flag count
- Negative-total flag count

## Partitioned Storage

Gold outputs use Hive-style monthly partitions:

```text
data/gold/pickup_zone_hourly/
  taxi_type=yellow/
    year=2024/
      month=01/metrics.parquet
      month=02/metrics.parquet
      month=03/metrics.parquet