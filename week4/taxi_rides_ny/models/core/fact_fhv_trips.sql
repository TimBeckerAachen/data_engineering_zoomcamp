{{ config(materialized='table') }}

with trips_fhv as (
    select * from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    trips_fhv.dispatching_base_num,
    trips_fhv.pickup_locationid,
    trips_fhv.dropoff_locationid,
    trips_fhv.pickup_datetime,
    trips_fhv.dropoff_datetime,
    trips_fhv.SR_Flag,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone

from trips_fhv
inner join dim_zones as pickup_zone
on trips_fhv.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_fhv.dropoff_locationid = dropoff_zone.locationid

WHERE DATE(pickup_datetime) BETWEEN  "2019-01-01" AND "2019-12-31"