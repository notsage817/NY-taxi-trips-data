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
    cast(vendor_id as integer) as vendor_id,
    cast(ratecode_id as integer) as rate_code_id,
    cast(pu_location_id as integer) as pickup_location_id,
    cast(do_location_id as integer) as dropoff_location_id,
    cast(service_type as string) as service_type,

    -- timestamps (standardized naming)
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,  -- tpep = Taxicab Passenger Enhancement Program (yellow taxis)
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(store_and_fwd_flag as string) as store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,

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
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    extracted_at
  
  FROM ingestion.trips
  WHERE 1=1
    AND lpep_pickup_datetime IS NOT NULL
    AND trip_distance > 0
    AND fare_amount >= 0
),

deduplicated AS (
  SELECT 
    -- identifiers
    vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    service_type,
    -- timestamps
    pickup_datetime,
    dropoff_datetime,
    -- trip info
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    -- payment info
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    congestion_surcharge,
    extracted_at,
    concat(vendor_id, service_type,pickup_location_id, dropoff_location_id, pickup_datetime ) as trip_id,
    ROW_NUMBER() OVER (PARTITION BY vendor_id, service_type,pickup_location_id, dropoff_location_id, pickup_datetime ORDER BY dropoff_datetime) AS rn
  FROM source_data
),

payment_lookup AS (
  SELECT 
    cast(payment_type_id as integer) as payment_type_id,
    cast(payment_type_name as string) as payment_name
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
  pl.payment_name as payment_type,
  d.extracted_at
FROM deduplicated d
left join payment_lookup pl on d.payment_type = pl.payment_type_id
where rn = 1