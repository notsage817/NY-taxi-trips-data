select *
from {{ref('int_trips')}}
where pickup_datetime >= '2021-01-01'