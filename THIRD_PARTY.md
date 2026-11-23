# Third-party components

Every outside image, library and tool used by the prototype, with its source and licence.
Entries marked (verify) still need the official licence page checked and linked.

## Container images

| Component | Version | Source | Licence |
| --- | --- | --- | --- |
| Redpanda | v24.3.1 | https://hub.docker.com/r/redpandadata/redpanda | Redpanda Business Source License; internal use permitted (verify current terms) |
| Redpanda Console | v2.8.0 | https://hub.docker.com/r/redpandadata/console | (verify) |
| CockroachDB | v24.3.0 | https://hub.docker.com/r/cockroachdb/cockroach | CockroachDB Software License; Enterprise Free for students and academic use, licence key required after 7 days (https://www.cockroachlabs.com/docs/stable/licensing-faqs) |
| Nginx | 1.27-alpine | https://hub.docker.com/_/nginx | BSD-2-Clause (verify) |
| Valkey (cache) | 8.0-alpine | https://hub.docker.com/r/valkey/valkey | BSD-3-Clause (verify) |
| Prometheus | v3.0.1 | https://hub.docker.com/r/prom/prometheus | Apache 2.0 (verify) |
| Grafana OSS | 11.3.0 | https://hub.docker.com/r/grafana/grafana-oss | AGPL-3.0; used unmodified as a separate service (verify) |
| Eclipse Mosquitto | 2.0.20 | https://hub.docker.com/_/eclipse-mosquitto | EPL-2.0 / EDL-1.0 (verify) |
| Python base image | 3.12-slim | https://hub.docker.com/_/python | Python Software Foundation License (verify) |

## Python libraries (ingest_api)

| Library | Version | Licence |
| --- | --- | --- |
| FastAPI | 0.165.0 | MIT (verify) |
| Uvicorn | 0.54.0 | BSD-3-Clause (verify) |
| aiokafka | 0.15.0 | Apache 2.0 (verify) |
| prometheus_client | 0.26.0 | Apache 2.0 (verify) |
| Pydantic | 2.13.5 | MIT (verify) |
| httpx (tests only) | 0.28.1 | BSD-3-Clause (verify) |
| pytest (tests only) | 9.1.1 | MIT (verify) |

## Python libraries (processor)

| Library | Version | Licence |
| --- | --- | --- |
| aiokafka | 0.15.0 | Apache 2.0 (verify) |
| psycopg | 3.3.9 | LGPL-3.0 (verify) |
| prometheus_client | 0.26.0 | Apache 2.0 (verify) |

## Python libraries (aggregator and read_api)

| Library | Version | Licence |
| --- | --- | --- |
| psycopg / psycopg-pool | 3.3.9 / 3.3.3 | LGPL-3.0 (verify) |
| redis (redis-py client) | 8.1.0 | MIT (verify) |
| FastAPI, Uvicorn, prometheus_client | as above | as above |

## Python libraries (sensor_bridge, sensor_aggregator, sensor_sim)

| Library | Version | Licence |
| --- | --- | --- |
| paho-mqtt | 2.1.0 | EPL-2.0 / EDL-1.0 (verify) |
| aiokafka, psycopg, prometheus_client | as above | as above |

## Tools

| Tool | Use | Licence |
| --- | --- | --- |
| Docker Desktop / Docker Compose | run the prototype | Docker Desktop subscription terms (free for personal and education use; verify) / Apache 2.0 |
