# Kestra Orchestration — NY Taxi Data Pipeline

This folder contains a **Kestra-based data pipeline** that extracts NYC taxi trip data from GitHub and loads it into datawarehouse (GCP here). It handles:

### What is Kestra?

[Kestra](https://kestra.io/) is an open-source **workflow orchestration platform** that lets you define, schedule, and monitor data pipelines as code using YAML. Key features:

- **Declarative YAML flows** — pipelines are defined as structured YAML files, no custom framework to learn
- **Rich plugin ecosystem** — built-in support for GCP, AWS, databases, HTTP, Python, Docker, and more
- **Web UI** — a visual interface for triggering, monitoring, and debugging executions
- **Key-Value store** — built-in secrets/config management used to store GCP credentials
- **Subflows** — flows can call other flows, enabling modular and reusable pipeline design

In this project, Kestra runs via Docker and uses PostgreSQL as its metadata store.

---

### Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | [Kestra](https://kestra.io/) v1.1 |
| Containerization | Docker + Docker Compose |
| Metadata Store | PostgreSQL 18 |
| Cloud Storage | Google Cloud Storage (GCS) |
| Data Warehouse | Google BigQuery |
| Scripting | Python 3.13+ (via `uv`) |
| In-flow queries | DuckDB (tutorial flow) |

---

### Folder Structure

```
kestra-orchestration/
├── docker-compose.yaml                        # Kestra + Postgres infrastructure
├── pyproject.toml                             # Python project metadata
├── main.py                                    # Placeholder entry point
└── flows/
    ├── 08_gcp_taxi.yaml                       # Core ETL: single taxi type/month
    ├── load_ny_taxi_gcp.yaml                  # Bulk orchestration: loops over 
    ├── verify_ny_taxi_connection.yaml         # Health check: tests local Postgres 
    └── 03_getting_started_data_pipeline.yaml  # Tutorial example (DuckDB + Python)
```

---

### Prerequisites

- A GCP project with a service account (BigQuery + GCS access)
- An external Docker network must exist, `ingestion_newyork_taxi_nw` created in /pipeline

### 1. Start Kestra

```bash
cd kestra-orchestration
docker compose up -d
```

Open the Kestra UI at [http://localhost:8080](http://localhost:8080)

### 2. Configure GCP Credentials (one-time)

Create a flow from the UI to store your GCP credentials in Kestra's key-value store.

### 3. Run the Pipeline

**Single month:**
`08_gcp_taxi.yaml` load data into datawarehouse with options one month at a time
- `taxi`: `yellow` or `green`
- `year`: e.g. `2019`
- `month`: e.g. `01`

**Bulk load (multiple months/years/taxi types):**
`load_ny_taxi_gcp.yaml` 
- Provide multi-select and loop over selected months 
- Call `08_gcp_taxi.yaml` for each month
