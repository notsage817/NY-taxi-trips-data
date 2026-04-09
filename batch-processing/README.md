# NY Taxi Trips — Batch Processing

NYC's Taxi & Limousine Commission publishes monthly trip records for every yellow cab ride in the city. This project takes those raw files, cleans and enriches them locally, then hands them off to a cloud pipeline that loads everything into BigQuery — where analysts can query millions of trips, track revenue trends, and explore demand by zone.

---

## What It Does

Raw monthly taxi data arrives inconsistently formatted and needs work before it can be trusted in reports. This module standardises the data — fixing column names, correcting types, and tagging each trip with a human-readable pickup and dropoff zone — then writes the result as compressed, cloud-ready files. Those files flow automatically through a cloud pipeline that loads them into BigQuery, where a transformation layer produces the final fact tables and aggregates used for analysis.

---

## How It Works

```
yellow_tripdata_YYYY-MM.parquet
          │
          ▼
  PySpark (local)
  clean · enrich · compress
          │
          ▼
  partitioned_taxi_data/
          │
          ▼
  Kestra → Google Cloud Storage → BigQuery
          │
          ▼
  dbt (SQL transformations)
          │
          ▼
  fct_trips · dim_zones · fct_monthly_zone_revenue
          │
          ▼
  BI dashboards / analysis
```

**PySpark** reads the raw file on your local machine and produces clean, compressed Parquet files. **Kestra** picks those files up, uploads them to **Google Cloud Storage**, and loads them into **BigQuery**. **dbt** then runs SQL models inside BigQuery that standardise, deduplicate, and aggregate the data into production-ready tables. The final tables are what dashboards and analysts query.

---

## How to Use It

**Prerequisites:** Python 3.13, Java 17 (OpenJDK), and `uv` installed on your machine.

```bash
uv install                                    # install dependencies
python test_spark.py                          # verify Spark is working
jupyter notebook batch_processing.ipynb       # open and run the pipeline
```

Place the raw input file (`yellow_tripdata_YYYY-MM.parquet`) in this directory before running the notebook. Processed output is written to `partitioned_taxi_data/` and is ready for cloud upload.

---

## Tech Stack

| Tool | Role |
|---|---|
| PySpark | Reads and transforms raw trip data on your local machine |
| Jupyter Notebook | Interactive environment for running the pipeline |
| uv | Python dependency management |
| Apache Parquet + Snappy | Compressed columnar format for processed output |
| Kestra | Orchestrates cloud ingestion — uploads files and triggers downstream jobs |
| Google Cloud Storage | Staging area between local output and BigQuery |
| Google BigQuery | Cloud data warehouse where all analytics runs |
| dbt | SQL transformation layer that builds the final analytical models in BigQuery |

---

## Project Structure

| File / Folder | Purpose |
|---|---|
| `batch_processing.ipynb` | Main pipeline notebook — run this to process a month of data |
| `test_spark.py` | Quick smoke test to confirm Spark is installed correctly |
| `taxi_zone_lookup.csv` | Reference table mapping location IDs to NYC zone names |
| `partitioned_taxi_data/` | Output directory — cleaned Parquet files ready for upload |
| `pyproject.toml` | Dependency manifest managed by `uv` |
| `.python-version` | Pins Python 3.13 |

The broader project also includes `dbt-cloud/` (SQL transformation models), `kestra-orchestration/` (cloud workflow definitions), and `airflow-orchestration/` (scheduling) — each owning a distinct stage of the pipeline.
