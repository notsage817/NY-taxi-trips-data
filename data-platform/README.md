## End-to-end data platform

### Ingestion data from API in python script
Read data in raw format with minimal transformation, like primary key or nor null IDs.

### Transformation in SQL
- Create trip_id (primary key) by concatenating the vender_id, pickup and dropoff locations and pickup time.
- Deduplicate the dataset
- Join with payment lookup table to substitute payment_type with real name

### Trips reporting
- Get daily count of trips and total revenue for each day
