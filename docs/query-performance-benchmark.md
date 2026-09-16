# Silver vs Gold Query Performance Benchmark

## Objective

This benchmark measures the benefit of querying the aggregated Gold layer instead of repeatedly scanning detailed Silver trip records.

Both queries answer the same business question:

> How many trips occurred for each pickup hour and pickup location across January through March 2024?

## Dataset

- Silver input rows: 9,551,872
- Gold input rows: 240,850
- Trips represented: 9,551,872
- Query result rows: 5,764
- Data format: Parquet
- Query engine: DuckDB 1.5.5
- Runtime: Python 3.12.10 on local Windows ARM64

## Method

The benchmark:

1. Executes each query once as a warm-up.
2. Executes each query 15 additional times.
3. Fetches the complete result for every execution.
4. Uses the median execution time to reduce the effect of outliers.
5. Verifies that Silver and Gold return identical result sets.
6. Verifies that both results represent all 9,551,872 accepted trips.

Run command:

```powershell
python scripts\benchmark_gold.py --runs 15