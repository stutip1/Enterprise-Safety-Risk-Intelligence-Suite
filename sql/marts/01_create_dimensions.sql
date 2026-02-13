create schema if not exists warehouse;

create or replace table warehouse.dim_date as
with dates as (
  select dateadd(day, seq4(), '2018-01-01') as calendar_date
  from table(generator(rowcount => 3650))
)
select
  to_number(to_char(calendar_date, 'YYYYMMDD')) as date_key,
  calendar_date,
  year(calendar_date) as year,
  quarter(calendar_date) as quarter,
  month(calendar_date) as month,
  weekofyear(calendar_date) as week_of_year,
  year(calendar_date) as fiscal_year
from dates;

create or replace table warehouse.dim_location as
select
  dense_rank() over (order by location_id) as location_key,
  location_id as location_token,
  site_code,
  site_name,
  region,
  country,
  business_unit
from staging.stg_locations;

create or replace table warehouse.dim_employee as
select
  dense_rank() over (order by employee_id) as employee_key,
  sha2(employee_id) as employee_token,
  role,
  shift,
  contractor_flag,
  datediff(month, hire_date, coalesce(termination_date, current_date())) as tenure_months
from staging.stg_employees;

create or replace table warehouse.dim_asset as
select
  dense_rank() over (order by asset_id) as asset_key,
  sha2(asset_id) as asset_id_token,
  asset_type,
  criticality
from staging.stg_assets;

create or replace table warehouse.dim_incident_type as
select
  dense_rank() over (order by incident_type) as incident_type_key,
  incident_type as incident_category,
  incident_type as osha_category,
  incident_type as classification
from staging.stg_incidents
group by incident_type;

create or replace table warehouse.dim_weather as
select
  dense_rank() over (order by weather_condition, temperature_c, precipitation_mm) as weather_key,
  weather_condition,
  temperature_c,
  precipitation_mm
from staging.stg_incidents
where weather_condition is not null
group by weather_condition, temperature_c, precipitation_mm;
