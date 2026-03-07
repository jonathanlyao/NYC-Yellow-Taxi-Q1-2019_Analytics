{{ config(materialized='view', schema='mart') }}

select
   f.*, 

    zpu.borough       as pu_borough,
    zpu.zone          as pu_zone,
    zpu.service_zone  as pu_service_zone,

    zdo.borough       as do_borough,
    zdo.zone          as do_zone,
    zdo.service_zone  as do_service_zone
    

from raw_core.fact_yellow_tripdata_2019_q1 as f
left join raw_core.dim_zone as zpu
  on f.pu_location_id = zpu.location_id
left join raw_core.dim_zone as zdo
  on f.do_location_id = zdo.location_id