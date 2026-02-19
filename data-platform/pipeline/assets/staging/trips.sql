/* @bruin
name: staging.trips

type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

@bruin */
WITH source_data AS (
  SELECT 
    DISTINCT
    -- identifiers (standardized naming for consistency across yellow/green)
    cast(vendorid as integer) as vendor_id,
    cast(ratecodeid as integer) as rate_code_id,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
    service_type,

    -- timestamps (standardized naming)
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,  -- tpep = Taxicab Passenger Enhancement Program (yellow taxis)
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(store_and_fwd_flag as string) as store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(1 as integer) as trip_type,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    extracted_at
  
  FROM ingestion.trips
  WHERE 1=1
    AND tpep_pickup_datetime IS NOT NULL
    AND trip_distance > 0
    AND fare_amount >= 0
),

deduplicated AS (
  SELECT 
    concat(vendor_id, service_type,pickup_location_id, dropoff_location_id, pickup_datetime ) as trip_id,
    *,
      -- synthetic primary key
    ROW_NUMBER() OVER (PARTITION BY vendor_id, service_type,pickup_location_id, dropoff_location_id, pickup_datetime ORDER BY dropoff_datetime) AS rn
  FROM source_data
),
payment_lookup AS (
  SELECT 
    cast(payment_type as integer) as payment_type,
    cast(payment_desc as string) as payment_description
  FROM ingestion.payment_lookup
)

SELECT 
  d.trip_id,
  d.vendor_id,
  d.rate_code_id,
  d.pickup_location_id,
  d.dropoff_location_id,
  d.service_type,
  d.pickup_datetime,
  d.dropoff_datetime,
  d.store_and_fwd_flag,
  d.passenger_count,
  d.trip_distance,
  d.trip_type,
  d.fare_amount,
  d.extra,
  d.mta_tax,
  d.tip_amount,
  d.tolls_amount,
  d.ehail_fee,
  d.improvement_surcharge,
  d.total_amount,
  pl.payment_description as payment_type,
  d.extracted_at
FROM deduplicated d
WHERE rn = 1
left join payment_lookup pl
  on d.payment_type = pl.payment_type