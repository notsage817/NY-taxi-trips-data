/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: view
  suggested_trategy: refresh
  # suggested strategy: time_interval
  #strategy: TODO
  # TODO: set to your report's date column
  #incremental_key: TODO
  # TODO: set to `date` or `timestamp`
  #time_granularity: TODO

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: pickup_date
    type: DATE
    description: The date of the pickup
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: The number of trips on a given date
    checks:
      - name: non_negative
  - name: total_revenue
    type: NUMERIC
    description: The total revenue from trips on a given date
    checks:
      - name: non_negative
@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT  date_trunc('day', pickup_datetime) as pickup_date, 
        count(*) as trip_count,
        sum(total_amount) as total_revenue
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1
