create or replace table warehouse.fact_near_miss as
select
  near_miss_id,
  emp.employee_key,
  loc.location_key,
  dt.date_key,
  asset.asset_key,
  potential_severity_score,
  corrective_action_flag,
  stg.source_system,
  stg.ingestion_ts
from staging.stg_near_miss stg
left join warehouse.dim_employee emp on sha2(stg.employee_id) = emp.employee_token
left join warehouse.dim_location loc on stg.location_id = loc.location_token
left join warehouse.dim_date dt on stg.near_miss_ts::date = dt.calendar_date
left join warehouse.dim_asset asset on sha2(stg.asset_id) = asset.asset_id_token;
