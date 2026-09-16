# March 2024 Complete End-to-End Pipeline Run

## Run Identification

- Run ID: `46904481-cc19-4b71-8bef-f43f3c134e7e`
- Partition: `yellow/2024/03`
- Status: Completed
- Source reused: Yes
- Overall quality passed: Yes
- Error message: None

## Pipeline Stages

The audited command executed the following stages:

1. Reused the validated March Bronze source.
2. Rebuilt the cleaned Silver partition.
3. Routed hard-rule failures to Quarantine.
4. Validated Silver quality and row reconciliation.
5. Built hourly pickup-zone Gold metrics.
6. Reconciled Gold trip counts with Silver.
7. Recorded the completed run and quality results in DuckDB.

## Command

```powershell
taxi-pipeline run --year 2024 --month 3