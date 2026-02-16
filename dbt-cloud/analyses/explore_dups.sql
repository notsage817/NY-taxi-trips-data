with with unioned_trips as (
    select * from {{ ref('int_trips_unioned') }}
),

ranked_trips as (
select *,
    row_number() over (
        partition by 
            vendor_id, 
            service_type,
            pickup_location_id, 
            dropoff_location_id,
            pickup_datetime, 
            dropoff_datetime, 
            passenger_count, 
            total_amount
        order by total_amount
        ) as rn
    from unioned_trips
)

select * from ranked_trips
where rn > 1