create or replace table warehouse.fact_exposure as
select
  exposure_id,
  emp.employee_key,
  loc.location_key,
  dt.date_key,
  hours_worked,
  stg.source_system,
  stg.ingestion_ts
from staging.stg_hours_worked stg
left join warehouse.dim_employee emp on sha2(stg.employee_id) = emp.employee_token
left join warehouse.dim_location loc on stg.location_id = loc.location_token
left join warehouse.dim_date dt on stg.work_date = dt.calendar_date;
