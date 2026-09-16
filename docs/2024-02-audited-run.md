# February 2024 Audited Pipeline Run

## Partition

- Taxi type: Yellow
- Year: 2024
- Month: February
- Partition key: `yellow/2024/02`

## First Run

- Run ID: `03098684-fc84-427e-a5c0-da566c7a4c37`
- Status: Completed
- Source reused: No
- Source rows: 3,007,526
- Silver rows: 3,006,695
- Quarantine rows: 831
- Quality gate passed: Yes
- Error message: None

## Repeat Run

- Run ID: `efd930a7-5d8c-4577-96fe-0e3470524a55`
- Status: Completed
- Source reused: Yes
- Source rows: 3,007,526
- Silver rows: 3,006,695
- Quarantine rows: 831
- Quality gate passed: Yes

The repeat run reused the existing validated Bronze file. Its row counts matched the first run, demonstrating repeatable processing and idempotent source ingestion.

## Row Reconciliation

```text
3,006,695 Silver rows + 831 Quarantine rows = 3,007,526 Source rows