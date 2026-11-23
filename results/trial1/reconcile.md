# Reconciliation: trial1 (PASS)

Method: Annex C. Drain: Prometheus sum(processor_lag_messages) == 0 three times, 5 s apart. Phantom window: 2026-11-23T11:05:22.221000+03:00 to 2026-11-23T11:09:59.101000+03:00.

| Check | Count | Required |
| --- | --- | --- |
| Records sent (distinct record_ids) | 3664 | |
| Records acknowledged (distinct) | 3664 | |
| **Lost** (acknowledged, not in database) | 0 | 0 |
| **Phantom** (in database, never sent) | 0 | 0 |
| **Duplicate record_id rows** | 0 | 0 |
| **Duplicate child + vaccine + dose** | 0 | 0 |
| Stored but never acknowledged | 0 | acceptable |
| Sent, never acknowledged, not stored | 0 | acceptable (client keeps them) |

## Exact queries

```sql
-- found: run for every sent record_id, 5,000 at a time
SELECT record_id FROM vaccination_records WHERE record_id = ANY(%s);
-- lost ones that were rejected instead
SELECT DISTINCT record_id FROM rejects WHERE record_id = ANY(%s);
-- phantom: rows received during the run window
SELECT record_id FROM vaccination_records WHERE received_at >= %s AND received_at <= %s;
SELECT count(*) FROM (SELECT record_id FROM vaccination_records GROUP BY record_id HAVING count(*) > 1) AS d;
SELECT count(*) FROM (SELECT child_id, vaccine_code, dose_no FROM vaccination_records GROUP BY 1, 2, 3 HAVING count(*) > 1) AS d;
```

Lost = acknowledged - found; Phantom = window rows - sent; computed in generator/app/reconcile.py from sent.log and ack.log.
