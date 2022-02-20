{{ config(materialized='view') }}

select
    -- identifiers
    dispatching_base_num,
    cast(PULocationID as integer) as pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    
    -- timestamps
    pickup_datetime,
    dropoff_datetime,
    
    -- trip info
    SR_Flag
from {{ source('staging','external_table_fhv_taxis_partitoned_clustered') }}
WHERE DATE(pickup_datetime) BETWEEN  "2019-01-01" AND "2019-12-31"

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}