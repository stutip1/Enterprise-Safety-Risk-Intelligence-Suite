create schema if not exists staging;

create or replace table staging.stg_incidents (
  incident_id string,
  employee_id string,
  location_id string,
  asset_id string,
  incident_type string,
  incident_ts timestamp_ntz,
  recordable_flag boolean,
  dart_flag boolean,
  days_away number(10, 2),
  restricted_days number(10, 2),
  medical_treatment_flag boolean,
  fatality_flag boolean,
  lost_time_flag boolean,
  cost_estimate number(18, 2),
  weather_condition string,
  temperature_c number(6, 2),
  precipitation_mm number(8, 2),
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string,
  is_valid boolean,
  validation_errors array
);

create or replace table staging.stg_near_miss (
  near_miss_id string,
  employee_id string,
  location_id string,
  asset_id string,
  near_miss_ts timestamp_ntz,
  potential_severity_score number(6, 2),
  corrective_action_flag boolean,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string,
  is_valid boolean,
  validation_errors array
);

create or replace table staging.stg_safety_observation (
  observation_id string,
  employee_id string,
  location_id string,
  asset_id string,
  observation_ts timestamp_ntz,
  observation_score number(6, 2),
  positive_observation_flag boolean,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string,
  is_valid boolean,
  validation_errors array
);

create or replace table staging.stg_employees (
  employee_id string,
  employee_name string,
  date_of_birth date,
  role string,
  shift string,
  contractor_flag boolean,
  hire_date date,
  termination_date date,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string,
  is_valid boolean,
  validation_errors array
);

create or replace table staging.stg_locations (
  location_id string,
  site_code string,
  site_name string,
  region string,
  country string,
  business_unit string,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string,
  is_valid boolean,
  validation_errors array
);

create or replace table staging.stg_assets (
  asset_id string,
  asset_type string,
  criticality string,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string,
  is_valid boolean,
  validation_errors array
);

create or replace table staging.stg_hours_worked (
  exposure_id string,
  employee_id string,
  location_id string,
  work_date date,
  hours_worked number(10, 2),
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string,
  is_valid boolean,
  validation_errors array
);
