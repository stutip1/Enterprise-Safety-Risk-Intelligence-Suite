create or replace table warehouse.fact_safety_observation as
select
  observation_id,
  emp.employee_key,
  loc.location_key,
  dt.date_key,
  asset.asset_key,
  observation_score,
  positive_observation_flag,
  stg.source_system,
  stg.ingestion_ts
from staging.stg_safety_observation stg
left join warehouse.dim_employee emp on sha2(stg.employee_id) = emp.employee_token
left join warehouse.dim_location loc on stg.location_id = loc.location_token
left join warehouse.dim_date dt on stg.observation_ts::date = dt.calendar_date
left join warehouse.dim_asset asset on sha2(stg.asset_id) = asset.asset_id_token;
