## Analytics Engineering in dbt cloud

This project transforms raw NYC Taxi & Limousine Commission (TLC) trip records into a clean, analytics-ready data warehouse using dbt. It ingests three data streams — Yellow taxi, Green taxi, and For-Hire Vehicle (FHV) trips — and applies a layered medallion architecture (staging → intermediate → marts) to produce:

- Deduplicated, type-cast trip records with surrogate keys
- Dimension tables for taxi zones and payment types and an incremental fact table for trips
- Allow different aggregations for further reporting

---

## How It Exactly Does It — Data Flow

```
BigQuery Raw Tables          Seeds (CSV)
       │                         │
       ▼                         ▼
  Staging Models            dim_zones / dim_payments
  (cast, rename,                 │
   filter nulls)                 │
       │                         │
       ▼                         │
  int_trips_unioned              │
  (UNION ALL yellow + green)     │
       │                         │
       ▼                         │
  int_trips                      │
  (surrogate key,                │
   deduplicate)                  │
       │                         │
       └──────────┬──────────────┘
                  ▼
             fct_trips
          (incremental MERGE)
                  │
                  ▼
      fct_monthly_zone_revenue
       (aggregated reporting)
```

**Step by step:**

1. **Sources** — Three raw BigQuery tables in the `ny_taxi` dataset: `yellow_tripdata`, `green_tripdata`, `fhv_tripdata_2019_ext`. Freshness is monitored (warn at 24h, error at 48h).

2. **Seeds** — Load any csv lookup files as static reference table:
   - `payment_type_lookup.csv` — 7 payment type codes and descriptions
   - `taxi_zone_lookup.csv` — 260+ NYC taxi zones with borough and service zone

3. **Staging** — Stage raw tables with coarsely filtering

   - Casts datatypes, rename columns and deal with null values 

4. **Intermediate** — Apply business logic:

    - Union all staging data into a pre-version of fact table joining necessary lookup files (payment_type, taxi_zoon, etc)
    - Create deterministic primary key with vendor_id+location_id+pickup_time and de-duplicate the fact table using window function

5. **Marts** — Last step to presentable analytical table:
    - Create dimension tables for taxi zones and payment types from dbt seeds
    - Build fact table from intermediate models
    - Build revenue reporting tables for different zones on monthly basis

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Warehouse | Google BigQuery |
| Transformation | dbt Cloud (v1.x) |
| SQL dialect | BigQuery Standard SQL |
| dbt package | `dbt-labs/dbt_utils` v1.1.1 |
| Source data | NYC TLC trip records |
| Connection profile | `dbt-cloud` |

---

## What is dbt and How to Use It

**dbt (data build tool)** is a transformation framework that lets you write SQL `SELECT` statements to pull data from datawarehouse and handles the rest — materializing views/tables, managing dependencies, running tests, and generating documentation. All transformations run inside your data warehouse (BigQuery in this case), so there is no data movement.

### Configuration files

- `dbt_project.yml` — project name, version, model paths, and default materializations
- `profiles.yml` (outside the repo, in `~/.dbt/`) — BigQuery credentials and connection settings
- `packages.yml` — external dbt package dependencies (run `dbt deps` to install)

---

## What Might Be Added to Improve It

| Improvement | Benefit |
|---|---|
| Add schema.yml tests (`not_null`, `accepted_values`, `relationships`) | Currently only one custom test exists; generic column-level tests would catch data quality issues earlier |
| Add dbt snapshots for `taxi_zone_lookup` | Would track slowly-changing dimension changes (zone renames, service zone reassignments) over time |
| Add exposure definitions | Documents which BI dashboards or downstream tools consume `fct_trips` and `fct_monthly_zone_revenue`, making lineage visible in dbt docs |
| Set up dbt Cloud Slim CI | Running `dbt build --select state:modified+` on pull requests would catch regressions before merging to production |
| Partition and cluster `fct_trips` | Partitioning by `pickup_datetime` and clustering by `service_type` or `pickup_location_id` would reduce BigQuery scan costs on common queries |
| Add more reporting marts | E.g., hourly demand patterns, airport trip analysis, or zone-to-zone trip flow tables to support more BI use cases |
| Parameterise the dev date filter | The hardcoded `2019-2021` dev filter in staging could be moved to `dbt_project.yml` vars for easier maintenance |
