# Taxi-Zone Dimension

## Purpose

Trip records identify pickup and drop-off areas with numeric location IDs. Those IDs are not meaningful to most analysts or business stakeholders.

The `dim_taxi_zone` dimension translates each location ID into:

- Borough
- Taxi zone
- Service-zone classification

This supports understandable reporting such as trip demand by borough, airport, or named neighborhood.

## Official Source

The dimension is built from the official NYC Taxi and Limousine Commission lookup:

```text
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv