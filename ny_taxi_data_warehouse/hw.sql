-- CREATE EXTERNAL TABLE
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.yellow-tripdata-2024-ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://yellow-ny-taxi-2024/*.parquet']
);

-- Load regular table
LOAD DATA OVERWRITE `ny_taxi.yellow-tripdata-2024`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://yellow-ny-taxi-2024/*.parquet']
);

-- Create table with partitioning and clustering
CREATE OR REPLACE TABLE `ny_taxi.yellow-trip-data-2024-optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `ny_taxi.yellow-tripdata-2024-ext`;

select count(distinct PULocationID) from `ny_taxi.yellow-trip-data-2024`;

select count(distinct PULocationID), count(distinct DOLocationID)
from `ny_taxi.yellow-trip-data-2024`;


select count(distinct VendorID)
from `ny_taxi.yellow_trip_2024_optimized`
where tpep_dropoff_datetime >= '2024-03-01'
  and tpep_dropoff_datetime <= '2024-03-15'

select count(distinct VendorID)
from `ny_taxi.yellow_taxi_2024`
where tpep_dropoff_datetime >= '2024-03-01'
  and tpep_dropoff_datetime <= '2024-03-15'

select count(*) from `ny_taxi.yellow_trip_2024_optimized`