{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        maximum_bytes_billed = '30000000000'
    )
}}
select t.trip_id,
    t.vendor_id,
    t.service_type,
    t.rate_code_id,

    t.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    t.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    t.pickup_datetime,
    t.dropoff_datetime,
    t.store_and_fwd_flag,

    t.passenger_count,
    t.trip_distance,
    t.trip_type,
    {{get_trip_duration_minutes('pickup_datetime', 'dropoff_datetime')}} as trip_dutation_minutes,

    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.ehail_fee,
    t.improvement_surcharge,
    t.total_amount

from {{ref('int_trips')}} as t
left join {{ref('dim_zones')}} as pz on t.pickup_location_id = pz.location_id
left join {{ref('dim_zones')}} as dz on t.dropoff_location_id = dz.location_id
