with src as (

	select * from {{ source('raw', 'taxi_zone_lookup') }}

), 

typed as (

	select 
	     cast(locationid as integer) as location_id, 
	     nullif(trim(borough), ' ') as borough, 
             nullif(trim(zone), ' ') as zone, 
             nullif(trim(service_zone), ' ') as service_zone
        from src
)

select * from typed