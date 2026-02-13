create schema if not exists raw;

create or replace table raw.incidents_api (
  payload variant,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string
);

create or replace table raw.near_miss_api (
  payload variant,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string
);

create or replace table raw.safety_observation_api (
  payload variant,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string
);

create or replace table raw.employees_api (
  payload variant,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string
);

create or replace table raw.locations_api (
  payload variant,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string
);

create or replace table raw.assets_api (
  payload variant,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string
);

create or replace table raw.hours_worked_api (
  payload variant,
  source_system string,
  ingestion_ts timestamp_ntz,
  record_hash string
);
