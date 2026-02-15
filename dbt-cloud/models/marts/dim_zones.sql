select 
    locationid as location_id,
    borough,
    zone,
    service_zone
from {{ref("taxi_zoon_lookup")}}