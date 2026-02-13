insert into staging.stg_incidents
select
  payload:incident_id::string as incident_id,
  payload:employee_id::string as employee_id,
  payload:location_id::string as location_id,
  payload:asset_id::string as asset_id,
  payload:incident_type::string as incident_type,
  payload:incident_ts::timestamp_ntz as incident_ts,
  payload:recordable_flag::boolean as recordable_flag,
  payload:dart_flag::boolean as dart_flag,
  payload:days_away::number(10, 2) as days_away,
  payload:restricted_days::number(10, 2) as restricted_days,
  payload:medical_treatment_flag::boolean as medical_treatment_flag,
  payload:fatality_flag::boolean as fatality_flag,
  payload:lost_time_flag::boolean as lost_time_flag,
  payload:cost_estimate::number(18, 2) as cost_estimate,
  payload:weather:condition::string as weather_condition,
  payload:weather:temperature_c::number(6, 2) as temperature_c,
  payload:weather:precipitation_mm::number(8, 2) as precipitation_mm,
  source_system,
  ingestion_ts,
  record_hash,
  true as is_valid,
  array_construct() as validation_errors
from raw.incidents_api;

insert into staging.stg_near_miss
select
  payload:near_miss_id::string,
  payload:employee_id::string,
  payload:location_id::string,
  payload:asset_id::string,
  payload:near_miss_ts::timestamp_ntz,
  payload:potential_severity_score::number(6, 2),
  payload:corrective_action_flag::boolean,
  source_system,
  ingestion_ts,
  record_hash,
  true,
  array_construct()
from raw.near_miss_api;

insert into staging.stg_safety_observation
select
  payload:observation_id::string,
  payload:employee_id::string,
  payload:location_id::string,
  payload:asset_id::string,
  payload:observation_ts::timestamp_ntz,
  payload:observation_score::number(6, 2),
  payload:positive_observation_flag::boolean,
  source_system,
  ingestion_ts,
  record_hash,
  true,
  array_construct()
from raw.safety_observation_api;

insert into staging.stg_employees
select
  payload:employee_id::string,
  payload:employee_name::string,
  payload:date_of_birth::date,
  payload:role::string,
  payload:shift::string,
  payload:contractor_flag::boolean,
  payload:hire_date::date,
  payload:termination_date::date,
  source_system,
  ingestion_ts,
  record_hash,
  true,
  array_construct()
from raw.employees_api;

insert into staging.stg_locations
select
  payload:location_id::string,
  payload:site_code::string,
  payload:site_name::string,
  payload:region::string,
  payload:country::string,
  payload:business_unit::string,
  source_system,
  ingestion_ts,
  record_hash,
  true,
  array_construct()
from raw.locations_api;

insert into staging.stg_assets
select
  payload:asset_id::string,
  payload:asset_type::string,
  payload:criticality::string,
  source_system,
  ingestion_ts,
  record_hash,
  true,
  array_construct()
from raw.assets_api;

insert into staging.stg_hours_worked
select
  payload:exposure_id::string,
  payload:employee_id::string,
  payload:location_id::string,
  payload:work_date::date,
  payload:hours_worked::number(10, 2),
  source_system,
  ingestion_ts,
  record_hash,
  true,
  array_construct()
from raw.hours_worked_api;
