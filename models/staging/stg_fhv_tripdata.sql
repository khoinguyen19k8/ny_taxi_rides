{{ config(materialized='view') }}

select
    -- Base License Numbers
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(Affiliated_base_number	as string) as affiliated_base_number,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- Location ID
    cast(PUlocationID as integer) as pulocationid,
    cast(DOlocationID as integer) as dolocationid,

    -- Others
    cast(SR_Flag as integer) as sr_flag
from {{ source('staging','external_fhv_tripdata_2019') }}
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default = True) %}

    limit 100
    
{% endif %}
