select * from {{ source('raw', 'green_tripdata') }}
where lpep_pickup_datetime between '2019-01-01' and '2019-01-03'