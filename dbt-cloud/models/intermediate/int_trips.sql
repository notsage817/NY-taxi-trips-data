with unioned as (
    select * from {{ ref("int_trips_unioned")}}
),

payment_type as (
    select * from {{ ref("payment_type_lookup")}}
),

enriched as (
    select 
        -- Generate unique trip identifier (surrogate key pattern)
        {{ dbt_utils.generate_surrogate_key(
            ['u.vendor_id', 'u.pickup_location_id','u.dropoff_location_id', 'u.dropoff_datetime', 'u.pickup_datetime', 'u.service_type']) }} as trip_id,
        --Identifiers
        u.vendor_id,
        u.service_type,
        u.rate_code_id,

        --Locations
        u.pickup_location_id,
        u.dropoff_location_id,
        
        --Timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        --Trip info
        u.store_and_fwd_flag,
        u.passenger_count,
        u.trip_distance,
        u.trip_type,

        -- Payment breakdown
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.total_amount,

    from unioned u 
    left join payment_type p on coalesce(u.payment_type, 0) = p.payment_type
)

select * from enriched

qualify row_number() over (
    partition by vendor_id, service_type, pickup_location_id, dropoff_location_id, pickup_datetime, dropoff_datetime, passenger_count
    order by total_amount
) = 1
