select 
    payment_type as payment_id,
    description as payment_type
from {{ ref('payment_type_lookup')}}
