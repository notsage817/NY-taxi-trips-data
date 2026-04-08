## Project Overview

### About the Data

This project ingests the [**NYC Taxi and Limousine Commission (TLC) Trip Record**](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset — one of the most well-known public datasets in data engineering. The taxi data is published monthly by the TLC and covers yellow cab, green cab, and for-hire vehicle trips across New York City.

Key fields in the dataset include:

| Field | Description |
|---|---|
| `VendorID` | Technology provider (1 = Creative Mobile, 2 = VeriFone) |
| `tpep_pickup_datetime` | Pickup timestamp |
| `tpep_dropoff_datetime` | Dropoff timestamp |
| `passenger_count` | Number of passengers |
| `trip_distance` | Trip distance in miles |
| `PULocationID` / `DOLocationID` | Pickup / dropoff taxi zone IDs |
| `payment_type` | Payment method (1 = Credit card, 2 = Cash, etc.) |
| `fare_amount` | Metered fare |
| `tip_amount` | Tip amount |
| `total_amount` | Total charged to passenger |

### Ingestion Architecture

Two ingestion paths are provided:

```
CSV files
  └─► ingest_data_pd.py  ─► Pandas DataFrame (chunked) ─► SQLAlchemy engine ─► PostgreSQL

bash
uv run python ingest_data_pd.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=1 \
  --chunksize=100000
```
```
Parquet files
  └─► ingest_data_pq.py  ─► Polars DataFrame (full load) ─► ADBC engine ─► PostgreSQL
                                                                 (≈10x faster than SQLAlchemy)
```

```bash
uv run python ingest_data_pq.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  # --datasource=yellow_tripdata \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=1
```

- **`ingest_data_pd.py`** uses `pandas` + `SQLAlchemy` and reads data in chunks — suitable for CSV files and compatible with most SQL backends.
- **`ingest_data_pq.py`** uses `polars` + `adbc-driver-postgresql` for direct, high-performance bulk loading of Parquet files. It also runs a `VACUUM ANALYZE` after each load to keep the table statistics up to date.

### Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.13 |
| Dependency management | [uv](https://github.com/astral-sh/uv) |
| Dataframe (fast path) | Polars |
| Dataframe (compat path) | Pandas |
| DB write engine (fast) | ADBC (`adbc-driver-postgresql`) |
| DB write engine (compat) | SQLAlchemy |
| Database driver | psycopg2 |
| Database | PostgreSQL 18 |
| DB admin UI | pgAdmin 4 |
| Containerisation | Docker + Docker Compose |
| CLI parsing | Click |

### Project Structure

```
pipeline/
├── ingest_data_pd.py     # CSV → Pandas → SQLAlchemy → PostgreSQL
├── ingest_data_pq.py     # Parquet → Polars → ADBC → PostgreSQL
├── Dockerfile            # Containerised pipeline image
├── docker-compose.yaml   # PostgreSQL + pgAdmin services
├── pyproject.toml        # Python dependencies (uv)
└── README.md             # This file
```
#### Containerize in docker compose
```
docker run -it \
  --network=ingestion_newyork_taxi_nw \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips \
    --year=2025 \
    --month=10
```
#### Manage Database with pgAdmin
Create a new container for pgAdmin and a network for pgAdmin to get access to postgreSQL database.
```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

#### Run postgreSQL DB in terminal
```bash
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
```
- Engine in SQLalchemy is commonly used especially for Pandas DataFrame, but it is not ideal and also requires loading by chunks of a file.
- Other files requires different dataframe such as Polars, which is more capable of handling large tables and allows 10X faster loading with 'adbc' engine.
