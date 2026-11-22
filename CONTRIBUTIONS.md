# Contributions

This is an individual project. All design decisions, configuration and code in this
repository are my own work, except the third-party components listed in THIRD_PARTY.md
and the AI assistance declared in AI_USE.md.

| Date | Work done |
| --- | --- |
| 2026-11-22 | Step 1: Redpanda (3 brokers), ingestion API, Nginx load balancer, test script |
| 2026-11-22 | Step 2: CockroachDB (3 nodes), schema and seed data, processor (validation, skew flag, dedup, rejects, DLQ) |
| 2026-11-22 | Step 3: aggregator with lease, Valkey cache, read API (roles + audit log), Prometheus + Grafana dashboard, processor lag metric, read-path check script |
