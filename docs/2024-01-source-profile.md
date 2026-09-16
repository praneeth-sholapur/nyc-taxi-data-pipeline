# January 2024 Source Profile

## Dataset

- NYC TLC Yellow Taxi Trip Records
- Partition: January 2024
- Source rows: 2,964,624

## Observed Quality Metrics

| Metric | Count |
| --- | ---: |
| Missing pickup timestamps | 0 |
| Missing drop-off timestamps | 0 |
| Drop-off before pickup | 56 |
| Zero or negative duration | 870 |
| Duration over 24 hours | 16 |
| Pickup outside January 2024 | 18 |
| Missing passenger count | 140,162 |
| Negative trip distance | 0 |
| Zero trip distance | 60,371 |
| Invalid pickup zone | 0 |
| Invalid drop-off zone | 0 |
| Negative total amount | 35,504 |

Quality-condition counts can overlap and must not be added together to
calculate rejected rows.

## Observed Ranges

- Minimum pickup: 2002-12-31 22:59:39
- Maximum pickup: 2024-02-01 00:01:15
- Minimum drop-off: 2002-12-31 23:05:41
- Maximum drop-off: 2024-02-02 13:56:52
- Average trip distance: 3.652
- Average total amount: 26.80

## Initial Quality Policy

### Quarantine

Records will be quarantined when:

- Pickup or drop-off timestamp is missing.
- Pickup occurs outside the expected source month.
- Drop-off is not later than pickup.
- Trip duration exceeds 24 hours.
- Trip distance is negative.
- Pickup or drop-off zone is outside 1 through 265.

### Preserve and Flag

Records will remain in silver but receive quality flags when:

- Passenger count is missing.
- Trip distance is zero.
- Total amount is negative.

Negative amounts may represent refunds or corrections. Zero-distance trips
may represent valid operational cases or meter-quality issues. Preserving
them prevents destructive assumptions while allowing downstream analysts
to filter them.

## Next Investigation

Exact duplicate records must be measured before finalizing the silver and
quarantine transformations.