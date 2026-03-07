{{ config(materialized='table', schema='core') }}

select 
	cast("LocationID" as integer) as location_id, 
	"Borough" as borough, 
	"Zone" as zone, 
	service_zone
	
	from {{ source('raw','taxi_zone_lookup') }}