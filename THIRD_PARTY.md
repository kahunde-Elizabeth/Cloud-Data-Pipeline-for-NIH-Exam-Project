# Third-party components

Every outside image, library and tool used by the prototype, with its source and licence.
Entries marked (verify) still need the official licence page checked and linked.

## Container images

| Component | Version | Source | Licence |
| --- | --- | --- | --- |
| Redpanda | v24.3.1 | https://hub.docker.com/r/redpandadata/redpanda | Redpanda Business Source License; internal use permitted (verify current terms) |
| Redpanda Console | v2.8.0 | https://hub.docker.com/r/redpandadata/console | (verify) |
| Nginx | 1.27-alpine | https://hub.docker.com/_/nginx | BSD-2-Clause (verify) |
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

## Tools

| Tool | Use | Licence |
| --- | --- | --- |
| Docker Desktop / Docker Compose | run the prototype | Docker Desktop subscription terms (free for personal and education use; verify) / Apache 2.0 |
