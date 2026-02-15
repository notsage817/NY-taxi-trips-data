with green_data as (
    select * , 
        'Green' as service_type
    from {{ ref("stg_green_tripdata")}}
),

yellow_data as (
    select * ,
        'Yellow' as service_type
    from {{ ref("stg_yellow_tripdata")}}
),

alltrips as (
    select * from green_data
    union all
    select * from yellow_data
)

select * from alltrips