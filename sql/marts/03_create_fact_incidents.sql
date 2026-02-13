create or replace table warehouse.fact_incidents as
select
  incident_id,
  emp.employee_key,
  loc.location_key,
  dt.date_key,
  asset.asset_key,
  it.incident_type_key,
  weather.weather_key,
  recordable_flag,
  dart_flag,
  days_away,
  restricted_days,
  medical_treatment_flag,
  fatality_flag,
  lost_time_flag,
  cost_estimate,
  stg.source_system,
  stg.ingestion_ts
from staging.stg_incidents stg
left join warehouse.dim_employee emp on sha2(stg.employee_id) = emp.employee_token
left join warehouse.dim_location loc on stg.location_id = loc.location_token
left join warehouse.dim_date dt on stg.incident_ts::date = dt.calendar_date
left join warehouse.dim_asset asset on sha2(stg.asset_id) = asset.asset_id_token
left join warehouse.dim_incident_type it on stg.incident_type = it.incident_category
left join warehouse.dim_weather weather
  on stg.weather_condition = weather.weather_condition
  and stg.temperature_c = weather.temperature_c
  and stg.precipitation_mm = weather.precipitation_mm;
