{{ config(materialized = 'table', schema = 'core') }}



With dates as (
	select 
		generate_series(date '2019-01-01', date '2019-03-31', interval '1 day')::date as date_day
)

select 
	date_day, 
	(to_char(date_day, 'YYYYMMDD'))::int as date_key, 

	extract(year from date_day)::int as year, 
	extract(quarter from date_day)::int as quarter,
	extract(month from date_day)::int as month,
	extract(day from date_day)::int as day,

	extract(isodow from date_day)::int as day_of_week,
	to_char(date_day, 'Day') as weekday_name,

	case when extract(isodow from date_day) in (6, 7) then 'Weekend' else 'Weekday' end as is_weekend, 
	
	to_char(date_day, 'YYYY-MM') as year_month,
	(extract(year from date_day)::int * 100 + extract(month from date_day)::int) as year_month_key, 

	to_char(date_day, 'Mon') as month_name, 

	date_trunc('week', date_day)::date as week_start_date,

	-- Holiday information join
	case when h.holiday_date is not null then 'Holiday' else 'Non-Holiday' end as is_holiday,
	h.holiday_name,
	h.holiday_type

	
from dates as d 
left join raw_core.dim_holiday as h
	on d.date_day = h.holiday_date
order by date_day



