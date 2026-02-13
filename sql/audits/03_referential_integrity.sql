insert into audits.data_quality_log
select
  uuid_string(),
  current_timestamp(),
  'warehouse.fact_incidents',
  'missing_employee_key',
  case when count_if(employee_key is null) = 0 then 'PASS' else 'FAIL' end,
  count_if(employee_key is null),
  'employee_key must resolve to dim_employee'
from warehouse.fact_incidents;

insert into audits.data_quality_log
select
  uuid_string(),
  current_timestamp(),
  'warehouse.fact_near_miss',
  'missing_location_key',
  case when count_if(location_key is null) = 0 then 'PASS' else 'FAIL' end,
  count_if(location_key is null),
  'location_key must resolve to dim_location'
from warehouse.fact_near_miss;
